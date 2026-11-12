package io.github.brunostsilva.webhooks.delivery

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import java.util.UUID
import org.springframework.stereotype.Component

/**
 * One circuit breaker per subscription: when an endpoint keeps failing (timeouts, 5xx, 429), its
 * deliveries stop being attempted for a while instead of tying up workers and connections, and the
 * other endpoints aren't affected. Breakers live in memory, so each instance has its own view.
 */
@Component
class EndpointCircuitBreakers(private val registry: CircuitBreakerRegistry) {

    fun forSubscription(subscriptionId: UUID): CircuitBreaker = registry.circuitBreaker(subscriptionId.toString())
}

/** Recorded by a circuit breaker as a failed call. */
class EndpointFailure(message: String) : RuntimeException(message, null, false, false)
