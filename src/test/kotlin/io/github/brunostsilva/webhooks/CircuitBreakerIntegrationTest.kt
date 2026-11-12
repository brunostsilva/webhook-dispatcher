package io.github.brunostsilva.webhooks

import io.github.brunostsilva.webhooks.delivery.EndpointCircuitBreakers
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import java.time.Duration
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired

class CircuitBreakerIntegrationTest : IntegrationTest() {

    @Autowired
    private lateinit var breakers: EndpointCircuitBreakers

    @Test
    fun `an endpoint that keeps failing stops being called, without affecting the others`() {
        val failing = receiver { TestReceiver.Reply(503) }
        val healthy = receiver()
        val failingSubscription = subscribe(failing.url, "order.shipped")
        subscribe(healthy.url, "order.shipped")
        val breaker = breakers.forSubscription(UUID.fromString(failingSubscription["id"] as String))

        val eventIds = (1..6).map { publishAccepted("order.shipped") }

        // The healthy endpoint gets every event.
        await().atMost(Duration.ofSeconds(20)).until { healthy.requests.size == 6 }
        // The failing one trips its breaker (4 calls, all failed, in the test profile)...
        await().atMost(Duration.ofSeconds(20)).until { breaker.state == CircuitBreaker.State.OPEN }
        Thread.sleep(300) // calls already on their way when it opened
        val callsWhenOpen = failing.requests.size

        // ...and then isn't called anymore: its deliveries wait for the circuit to close, without
        // using up their attempts.
        Thread.sleep(1000) // longer than any retry would wait in the test profile
        assertThat(failing.requests).hasSize(callsWhenOpen)
        val waiting = eventIds.flatMap(::deliveriesOf).filter { it["subscriptionId"] == failingSubscription["id"] }
        assertThat(waiting).hasSize(6)
        waiting.forEach { assertThat(it["status"]).isIn("PENDING", "IN_FLIGHT") }
    }
}
