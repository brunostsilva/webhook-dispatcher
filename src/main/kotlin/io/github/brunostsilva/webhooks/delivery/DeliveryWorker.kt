package io.github.brunostsilva.webhooks.delivery

import io.github.brunostsilva.webhooks.config.DispatcherProperties
import java.time.Clock
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import org.springframework.stereotype.Component

/**
 * Claims due deliveries in batches and processes them concurrently: at most `max-concurrency` at a
 * time, and at most `max-concurrency-per-subscription` per endpoint, so one slow endpoint can't take
 * every slot. Any number of instances can run it (see [DeliveryRepository.claim]).
 *
 * On shutdown it stops claiming and lets the current batch finish, up to `shutdown-timeout`; whatever
 * is still in flight after that is cancelled and claimed again once its lease expires.
 */
@Component
class DeliveryWorker(
    private val deliveries: DeliveryRepository,
    private val processor: DeliveryProcessor,
    private val signal: WorkerSignal,
    private val clock: Clock,
    properties: DispatcherProperties,
) : SmartLifecycle {

    private val log = LoggerFactory.getLogger(javaClass)
    private val settings = properties.worker
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("delivery-worker"))
    private val slots = Semaphore(settings.maxConcurrency)
    private val endpointSlots = ConcurrentHashMap<UUID, Semaphore>()

    @Volatile
    private var loop: Job? = null

    @Volatile
    private var stopping = false

    override fun start() {
        if (!settings.enabled) return
        stopping = false
        loop = scope.launch { run() }
        log.info("Delivery worker started")
    }

    override fun stop() {
        val running = loop ?: return
        stopping = true
        signal.wake()
        runBlocking {
            if (withTimeoutOrNull(settings.shutdownTimeout.toMillis()) { running.join() } == null) {
                log.warn("Delivery worker didn't finish within {}, cancelling", settings.shutdownTimeout)
                running.cancelAndJoin()
            }
        }
        loop = null
        log.info("Delivery worker stopped")
    }

    override fun isRunning(): Boolean = loop?.isActive == true

    private suspend fun run() {
        while (!stopping) {
            val claimed = try {
                processBatch()
            } catch (cancellation: CancellationException) {
                throw cancellation
            } catch (error: Exception) {
                log.error("Delivery batch failed, retrying after the poll interval", error)
                0
            }
            // A full batch means more may be waiting: claim again right away.
            if (claimed < settings.batchSize && !stopping) signal.await(settings.pollInterval)
        }
    }

    private suspend fun processBatch(): Int {
        val now = clock.instant()
        val batch = deliveries.claim(now, now.plus(settings.lease), settings.batchSize)
        coroutineScope {
            for (job in batch) {
                launch {
                    val endpoint = endpointSlots.computeIfAbsent(job.subscriptionId) {
                        Semaphore(settings.maxConcurrencyPerSubscription)
                    }
                    endpoint.withPermit {
                        slots.withPermit { processSafely(job) }
                    }
                }
            }
        }
        return batch.size
    }

    private suspend fun processSafely(job: DeliveryJob) {
        try {
            processor.process(job)
        } catch (cancellation: CancellationException) {
            throw cancellation
        } catch (error: Exception) {
            // Left IN_FLIGHT: it's claimed again when its lease expires.
            log.error("Processing delivery {} failed", job.deliveryId, error)
        }
    }
}
