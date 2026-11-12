package io.github.brunostsilva.webhooks

import java.util.UUID
import java.util.concurrent.Executors
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.http.MediaType
import org.springframework.test.web.reactive.server.expectBody

class IdempotencyIntegrationTest : IntegrationTest() {

    @Test
    fun `repeating a request returns the original event and delivers it once`() {
        val endpoint = receiver()
        subscribe(endpoint.url, "payout.sent")
        val key = UUID.randomUUID().toString()
        val data = mapOf("payoutId" to "po_1")

        val first = publish("payout.sent", data, key)
            .expectStatus().isAccepted
            .expectHeader().valueEquals("Idempotent-Replayed", "false")
            .expectBody<Map<String, Any?>>().returnResult().responseBody!!
        val replay = publish("payout.sent", data, key)
            .expectStatus().isOk
            .expectHeader().valueEquals("Idempotent-Replayed", "true")
            .expectBody<Map<String, Any?>>().returnResult().responseBody!!

        assertThat(replay["id"]).isEqualTo(first["id"])
        assertThat(replay["deliveries"]).isEqualTo(1)
        awaitDelivery(first["id"] as String) { it["status"] == "DELIVERED" }
        Thread.sleep(300)
        assertThat(endpoint.requests).hasSize(1)
    }

    @Test
    fun `reusing a key for a different request is rejected`() {
        val key = UUID.randomUUID().toString()
        publish("payout.sent", mapOf("payoutId" to "po_1"), key).expectStatus().isAccepted

        publish("payout.sent", mapOf("payoutId" to "po_2"), key)
            .expectStatus().isEqualTo(422)
            .expectHeader().contentType(MediaType.APPLICATION_PROBLEM_JSON)
    }

    @Test
    fun `concurrent requests with the same key create a single event`() {
        val endpoint = receiver()
        subscribe(endpoint.url, "payout.failed")
        val key = UUID.randomUUID().toString()

        val responses = Executors.newVirtualThreadPerTaskExecutor().use { executor ->
            (1..10).map {
                executor.submit<Pair<Int, Any?>> {
                    val result = publish("payout.failed", mapOf("payoutId" to "po_3"), key)
                        .expectBody<Map<String, Any?>>().returnResult()
                    result.status.value() to result.responseBody!!["id"]
                }
            }.map { it.get() }
        }

        assertThat(responses.map { it.second }.distinct()).hasSize(1)
        assertThat(responses.count { it.first == 202 }).isEqualTo(1)
        assertThat(responses.count { it.first == 200 }).isEqualTo(9)
        awaitDelivery(responses.first().second as String) { it["status"] == "DELIVERED" }
        Thread.sleep(300)
        assertThat(endpoint.requests).hasSize(1)
    }

    @Test
    fun `the Idempotency-Key header is required`() {
        api.post().uri("/events")
            .bodyValue(mapOf("type" to "payout.sent", "data" to mapOf("payoutId" to "po_4")))
            .exchange()
            .expectStatus().isBadRequest
    }
}
