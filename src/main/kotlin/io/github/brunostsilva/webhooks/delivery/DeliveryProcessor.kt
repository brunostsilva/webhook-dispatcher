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
        // The lease ran from the claim; it's renewed now that the delivery is about to be sent, so it
        // covers the whole attempt. If the claim was lost meanwhile (the lease expired and another
        // worker took the delivery over), it isn't sent: that worker sends it.
        val start = clock.instant()
        if (!deliveries.renewLease(job, start.plus(properties.worker.lease), start)) {
            log.atInfo()
                .setMessage("Delivery {} of event {} was taken over by another worker before being sent: skipped")
                .addArgument(job.deliveryId)
                .addArgument(job.eventId)
                .addKeyValue("webhook.delivery_id", job.deliveryId)
                .addKeyValue("webhook.event_id", job.eventId)
                .log()
            return
        }

        val breaker = breakers.forSubscription(job.subscriptionId)
        if (!breaker.tryAcquirePermission()) {
            // The endpoint is known to be down: hand the delivery back without using up an attempt.
            deliveries.postpone(job, start.plus(properties.circuitBreaker.waitDurationInOpenState), start)
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
        if (!deliveries.complete(job, result, transition, now)) {
            // Can only happen if the attempt outlived its lease: the outcome belongs to the new owner.
            log.atWarn()
                .setMessage("Delivery {} of event {} lost its claim during attempt {} ({}): outcome not recorded")
                .addArgument(job.deliveryId)
                .addArgument(job.eventId)
                .addArgument(attempt)
                .addArgument(response.describe())
                .addKeyValue("webhook.delivery_id", job.deliveryId)
                .addKeyValue("webhook.event_id", job.eventId)
                .log()
            return
        }
        metrics.attempt(transition, duration)

        logAttempt(job, attempt, response, duration, transition)
    }

    /** One line per attempt: INFO when delivered or retried, WARN when refused or given up. */
    private fun logAttempt(job: DeliveryJob, attempt: Int, response: WebhookResponse, duration: Duration, transition: Transition) {
        val event = when (transition) {
            Transition.Delivered -> log.atInfo()
                .setMessage("Delivery {} of event {} to subscription {} delivered on attempt {} ({}, {} ms)")
            is Transition.Retry -> log.atInfo()
                .setMessage("Delivery {} of event {} to subscription {} failed on attempt {} ({}, {} ms), retrying at {}")
            Transition.Failed -> log.atWarn()
                .setMessage("Delivery {} of event {} to subscription {} refused on attempt {} ({}, {} ms), not retried")
            Transition.Dead -> log.atWarn()
                .setMessage("Delivery {} of event {} to subscription {} gave up after attempt {} ({}, {} ms)")
        }
        event.addArgument(job.deliveryId)
            .addArgument(job.eventId)
            .addArgument(job.subscriptionId)
            .addArgument(attempt)
            .addArgument(response.describe())
            .addArgument(duration.toMillis())
        if (transition is Transition.Retry) event.addArgument(transition.at)
        event.addKeyValue("webhook.delivery_id", job.deliveryId)
            .addKeyValue("webhook.event_id", job.eventId)
            .addKeyValue("webhook.subscription_id", job.subscriptionId)
            .addKeyValue("webhook.attempt", attempt)
            .addKeyValue("webhook.delivery_status", transition.status)
            .addKeyValue("webhook.duration_ms", duration.toMillis())
        if (response is WebhookResponse.Status) event.addKeyValue("http.response.status_code", response.code)
        event.log()
    }

    private fun WebhookResponse.describe(): String = when (this) {
        is WebhookResponse.Status -> "HTTP $code"
        is WebhookResponse.Failure -> error
    }

    private companion object {
        const val MAX_ERROR_LENGTH = 500
    }
}
