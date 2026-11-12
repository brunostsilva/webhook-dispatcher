package io.github.brunostsilva.webhooks

import io.github.brunostsilva.webhooks.api.ApiKeyFilter
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.http.MediaType
import org.springframework.test.web.reactive.server.WebTestClient
import org.springframework.test.web.reactive.server.expectBody

class ApiIntegrationTest : IntegrationTest() {

    private val anonymous: WebTestClient by lazy { WebTestClient.bindToServer().baseUrl(baseUrl).build() }

    @Test
    fun `API calls need the API key`() {
        anonymous.get().uri("/subscriptions/{id}", UUID.randomUUID())
            .exchange()
            .expectStatus().isUnauthorized
            .expectHeader().contentType(MediaType.APPLICATION_PROBLEM_JSON)

        anonymous.post().uri("/events")
            .header(ApiKeyFilter.HEADER, "wrong-key")
            .header("Idempotency-Key", UUID.randomUUID().toString())
            .bodyValue(mapOf("type" to "payment.succeeded", "data" to mapOf("paymentId" to "pay_1")))
            .exchange()
            .expectStatus().isUnauthorized
    }

    @Test
    fun `health probes and metrics are open, and the metrics cover deliveries and circuit breakers`() {
        anonymous.get().uri("/actuator/health/liveness").exchange()
            .expectStatus().isOk
            .expectBody().jsonPath("$.status").isEqualTo("UP")
        anonymous.get().uri("/actuator/health/readiness").exchange()
            .expectStatus().isOk
            .expectBody().jsonPath("$.status").isEqualTo("UP")

        val endpoint = receiver()
        subscribe(endpoint.url, "metrics.check")
        awaitDelivery(publishAccepted("metrics.check")) { it["status"] == "DELIVERED" }

        val metrics = anonymous.get().uri("/actuator/prometheus").exchange()
            .expectStatus().isOk
            .expectBody<String>().returnResult().responseBody!!
        assertThat(metrics)
            .contains("webhook_delivery_attempts_seconds_count{outcome=\"delivered\"")
            .contains("resilience4j_circuitbreaker_state")
    }

    @Test
    fun `a subscription needs a valid http(s) URL and at least one event type`() {
        val invalid = listOf(
            mapOf("url" to "ftp://example.com/hooks", "eventTypes" to listOf("a.b")),
            mapOf("url" to "not a url", "eventTypes" to listOf("a.b")),
            mapOf("url" to "https://user:password@example.com/hooks", "eventTypes" to listOf("a.b")),
            mapOf("url" to "https://example.com/hooks", "eventTypes" to emptyList<String>()),
            mapOf("url" to "https://example.com/hooks", "eventTypes" to listOf(" ")),
        )
        for (request in invalid) {
            api.post().uri("/subscriptions").bodyValue(request).exchange()
                .expectStatus().isBadRequest
                .expectHeader().contentType(MediaType.APPLICATION_PROBLEM_JSON)
        }
    }

    @Test
    fun `a subscription's secret is only shown when it's created`() {
        val created = subscribe("https://example.com/hooks", "secret.check")
        assertThat(created["secret"] as String).startsWith("whsec_")

        val fetched = api.get().uri("/subscriptions/{id}", created["id"]).exchange()
            .expectStatus().isOk
            .expectBody<Map<String, Any?>>().returnResult().responseBody!!
        assertThat(fetched)
            .doesNotContainKey("secret")
            .containsEntry("url", "https://example.com/hooks")
            .containsEntry("eventTypes", listOf("secret.check"))
    }

    @Test
    fun `unknown resources are 404, and only failed or dead deliveries can be retried`() {
        val unknown = UUID.randomUUID()
        api.get().uri("/subscriptions/{id}", unknown).exchange().expectStatus().isNotFound
        api.get().uri("/events/{id}/deliveries", unknown).exchange().expectStatus().isNotFound
        api.post().uri("/deliveries/{id}/retry", unknown).exchange().expectStatus().isNotFound

        val endpoint = receiver()
        subscribe(endpoint.url, "retry.check")
        val delivered = awaitDelivery(publishAccepted("retry.check")) { it["status"] == "DELIVERED" }
        api.post().uri("/deliveries/{id}/retry", delivered["id"]).exchange()
            .expectStatus().isEqualTo(409)
            .expectHeader().contentType(MediaType.APPLICATION_PROBLEM_JSON)
    }
}
