package io.github.brunostsilva.webhooks.delivery

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import java.time.Duration
import org.springframework.stereotype.Component

/**
 * `webhook.delivery.attempts` (timer, by outcome: delivered, retry, failed, dead) and
 * `webhook.delivery.postponed` (counter: deliveries held back while their endpoint's circuit is open).
 * Tags never include the subscription or the URL, to keep the number of series bounded.
 */
@Component
class DeliveryMetrics(private val registry: MeterRegistry) {

    private val postponedDeliveries = Counter.builder("webhook.delivery.postponed")
        .description("Deliveries postponed because their endpoint's circuit breaker was open")
        .register(registry)

    fun attempt(transition: Transition, duration: Duration) {
        val outcome = when (transition) {
            Transition.Delivered -> "delivered"
            Transition.Failed -> "failed"
            Transition.Dead -> "dead"
            is Transition.Retry -> "retry"
        }
        Timer.builder("webhook.delivery.attempts")
            .description("HTTP attempts to deliver a webhook, by outcome")
            .tag("outcome", outcome)
            .register(registry)
            .record(duration)
    }

    fun postponed() = postponedDeliveries.increment()
}
