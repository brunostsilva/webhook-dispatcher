package io.github.brunostsilva.webhooks

import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.boot.test.system.CapturedOutput
import org.springframework.boot.test.system.OutputCaptureExtension
import org.springframework.test.web.reactive.server.expectBody

@ExtendWith(OutputCaptureExtension::class)
class LoggingIntegrationTest : IntegrationTest() {

    @Test
    fun `a delivery is logged with ids and outcome, never with the payload, the secret or the URL`(output: CapturedOutput) {
        val endpoint = receiver()
        val subscription = subscribe(endpoint.url, "logging.check")
        val cardNumber = "4111111111111111"
        val eventId = publish("logging.check", mapOf("card" to cardNumber))
            .expectStatus().isAccepted
            .expectBody<Map<String, Any?>>().returnResult().responseBody!!["id"] as String

        val delivery = awaitDelivery(eventId) { it["status"] == "DELIVERED" }

        // Loggers are asynchronous: lines show up shortly after the call.
        val delivered = "Delivery ${delivery["id"]} of event $eventId to subscription ${subscription["id"]} delivered on attempt 1 (HTTP 200"
        await().atMost(Duration.ofSeconds(5)).until { output.out.contains(delivered) }
        assertThat(output.out)
            .contains("Subscription ${subscription["id"]} created for [logging.check] on 127.0.0.1")
            .contains("Event $eventId of type logging.check published with 1 delivery(ies)")
            .doesNotContain(cardNumber)
            .doesNotContain(subscription["secret"] as String)
            .doesNotContain(endpoint.url)
    }

    @Test
    fun `retries and a dead letter are logged, the dead letter as a warning`(output: CapturedOutput) {
        val endpoint = receiver { TestReceiver.Reply(500) }
        subscribe(endpoint.url, "logging.failure")
        val eventId = publishAccepted("logging.failure")

        awaitDelivery(eventId) { it["status"] == "DEAD" }

        await().atMost(Duration.ofSeconds(5)).until { output.out.contains("gave up after attempt 3 (HTTP 500") }
        val lines = output.out.lines().filter { it.contains("of event $eventId") }
        assertThat(lines.filter { it.contains("failed on attempt") && it.contains(" INFO ") }).hasSize(2)
        assertThat(lines.filter { it.contains("gave up after attempt 3") && it.contains(" WARN ") }).hasSize(1)
    }
}
