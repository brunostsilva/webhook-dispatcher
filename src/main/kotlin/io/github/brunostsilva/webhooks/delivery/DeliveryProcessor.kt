package io.github.brunostsilva.webhooks.delivery

import io.github.brunostsilva.webhooks.config.DispatcherProperties
import java.time.Clock
import java.time.Duration
import java.util.concurrent.TimeUnit
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

/** Makes one attempt at a claimed delivery and records where it goes next. */
@Component
class DeliveryProcessor(
    private val deliveries: DeliveryRepository,
    private val sender: WebhookSender,
    private val breakers: EndpointCircuitBreakers,
    private val retryPolicy: RetryPolicy,
    private val metrics: DeliveryMetrics,
    private val clock: Clock,
    private val properties: DispatcherProperties,
) {

    private val log = LoggerFactory.getLogger(javaClass)

    suspend fun process(job: DeliveryJob) {
        val breaker = breakers.forSubscription(job.subscriptionId)
        if (!breaker.tryAcquirePermission()) {
            // The endpoint is known to be down: hand the delivery back without using up an attempt.
            val now = clock.instant()
            deliveries.postpone(job.deliveryId, now.plus(properties.circuitBreaker.waitDurationInOpenState), now)
            metrics.postponed()
            log.debug("Circuit open for subscription {}, postponed delivery {}", job.subscriptionId, job.deliveryId)
            return
        }

        val started = System.nanoTime()
        val response = sender.send(job)
        val duration = Duration.ofNanos(System.nanoTime() - started)
        val verdict = retryPolicy.classify(response)

        // A refused request still means the endpoint is up: only retryable failures trip the breaker.
        when (verdict) {
            is Verdict.Retry ->
                breaker.onError(duration.toNanos(), TimeUnit.NANOSECONDS, EndpointFailure(response.describe()))
            Verdict.Delivered, Verdict.Rejected -> breaker.onSuccess(duration.toNanos(), TimeUnit.NANOSECONDS)
        }

        val attempt = job.attempts + 1
        val now = clock.instant()
        val transition = when (verdict) {
            Verdict.Delivered -> Transition.Delivered
            Verdict.Rejected -> Transition.Failed
            is Verdict.Retry ->
                if (attempt >= job.attemptLimit) {
                    Transition.Dead
                } else {
                    Transition.Retry(now.plus(retryPolicy.backoff(attempt, verdict.retryAfter)))
                }
        }
        val result = AttemptResult(
            number = attempt,
            statusCode = (response as? WebhookResponse.Status)?.code,
            error = (response as? WebhookResponse.Failure)?.error?.take(MAX_ERROR_LENGTH),
            duration = duration,
        )
        deliveries.complete(job, result, transition, now)
        metrics.attempt(transition, duration)

        when (transition) {
            Transition.Dead -> log.warn(
                "Delivery {} of event {} gave up after {} attempts: {}",
                job.deliveryId, job.eventId, attempt, response.describe(),
            )
            Transition.Failed -> log.warn(
                "Delivery {} of event {} was refused: {}", job.deliveryId, job.eventId, response.describe(),
            )
            else -> log.debug("Delivery {} attempt {}: {}", job.deliveryId, attempt, response.describe())
        }
    }

    private fun WebhookResponse.describe(): String = when (this) {
        is WebhookResponse.Status -> "HTTP $code"
        is WebhookResponse.Failure -> error
    }

    private companion object {
        const val MAX_ERROR_LENGTH = 500
    }
}
