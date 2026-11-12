package io.github.brunostsilva.webhooks.delivery

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import java.util.UUID
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

/**
 * One circuit breaker per subscription: when an endpoint keeps failing (timeouts, 5xx, 429), its
 * deliveries stop being attempted for a while instead of tying up workers and connections, and the
 * other endpoints aren't affected. Breakers live in memory, so each instance has its own view.
 */
@Component
class EndpointCircuitBreakers(private val registry: CircuitBreakerRegistry) {

    private val log = LoggerFactory.getLogger(javaClass)

    init {
        // Logs every state change of every breaker: WARN when an endpoint's circuit opens.
        registry.eventPublisher.onEntryAdded { added ->
            val breaker = added.addedEntry
            breaker.eventPublisher.onStateTransition { change ->
                val transition = change.stateTransition
                val event = if (transition.toState == CircuitBreaker.State.OPEN) log.atWarn() else log.atInfo()
                event.setMessage("Circuit for subscription {} went from {} to {}")
                    .addArgument(breaker.name)
                    .addArgument(transition.fromState)
                    .addArgument(transition.toState)
                    .addKeyValue("webhook.subscription_id", breaker.name)
                    .addKeyValue("webhook.circuit_state", transition.toState)
                    .log()
            }
        }
    }

    fun forSubscription(subscriptionId: UUID): CircuitBreaker = registry.circuitBreaker(subscriptionId.toString())
}

/** Recorded by a circuit breaker as a failed call. */
class EndpointFailure(message: String) : RuntimeException(message, null, false, false)
