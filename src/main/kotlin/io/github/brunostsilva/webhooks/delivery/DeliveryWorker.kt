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
 * Keeps up to `max-concurrency` deliveries in flight, at most `max-concurrency-per-subscription` per
 * endpoint. It claims only as many deliveries as it has free slots, skipping endpoints already at
 * their limit, and claims again as soon as a slot frees up: a slow endpoint holds its own slots, never
 * the others'. Any number of instances can run it (see [DeliveryRepository.claim]).
 *
 * On shutdown it stops claiming and lets in-flight deliveries finish, up to `shutdown-timeout`;
 * whatever is still running after that is cancelled and claimed again once its lease expires.
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

    /** Returns once stopping and every delivery it started has finished. */
    private suspend fun run(): Unit = coroutineScope {
        while (!stopping) {
            val capacity = minOf(settings.batchSize, slots.availablePermits)
            val claimed = if (capacity > 0) claim(capacity) else emptyList()
            for (job in claimed) {
                // Reserved before launching, so there are never more than max-concurrency in flight.
                slots.acquire()
                launch {
                    try {
                        endpointSlot(job.subscriptionId).withPermit { processSafely(job) }
                    } finally {
                        slots.release()
                        signal.wake() // a slot is free: claim again without waiting for the poll interval
                    }
                }
            }
            // A full claim means more may be due: claim again right away. Otherwise wait for a new
            // event, a freed slot or the poll interval.
            if (capacity == 0 || claimed.size < capacity) signal.await(settings.pollInterval)
        }
    }

    private suspend fun claim(capacity: Int): List<DeliveryJob> =
        try {
            val now = clock.instant()
            val busy = endpointSlots.filterValues { it.availablePermits == 0 }.keys
            deliveries.claim(now, now.plus(settings.lease), capacity, settings.maxConcurrencyPerSubscription, busy)
        } catch (cancellation: CancellationException) {
            throw cancellation
        } catch (error: Exception) {
            log.error("Claiming deliveries failed, retrying after the poll interval", error)
            emptyList()
        }

    private fun endpointSlot(subscriptionId: UUID): Semaphore =
        endpointSlots.computeIfAbsent(subscriptionId) { Semaphore(settings.maxConcurrencyPerSubscription) }

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
