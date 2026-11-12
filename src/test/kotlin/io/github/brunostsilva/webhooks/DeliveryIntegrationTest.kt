package io.github.brunostsilva.webhooks

import io.github.brunostsilva.webhooks.delivery.WebhookSigner
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.within
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.springframework.test.web.reactive.server.expectBody

class DeliveryIntegrationTest : IntegrationTest() {

    @Test
    fun `delivers an event to each matching subscription, signed per Standard Webhooks`() {
        val matching = receiver()
        val other = receiver()
        val subscription = subscribe(matching.url, "payment.succeeded")
        subscribe(other.url, "refund.created")

        val event = publish("payment.succeeded", mapOf("paymentId" to "pay_123", "amount" to 4200))
            .expectStatus().isAccepted
            .expectBody<Map<String, Any?>>().returnResult().responseBody!!
        assertThat(event["deliveries"]).isEqualTo(1)
        val eventId = event["id"] as String

        val delivery = awaitDelivery(eventId) { it["status"] == "DELIVERED" }
        assertThat(delivery["attempts"]).isEqualTo(1)
        assertThat(history(delivery).single()["statusCode"]).isEqualTo(200)

        val request = matching.requests.single()
        val timestamp = request.headers.getValue("webhook-timestamp").toLong()
        assertThat(request.headers["webhook-id"]).isEqualTo(eventId)
        assertThat(timestamp).isCloseTo(Instant.now().epochSecond, within(60L))
        assertThat(request.headers["webhook-signature"])
            .isEqualTo(WebhookSigner.sign(subscription["secret"] as String, eventId, timestamp, request.body))
        assertThat(request.headers["content-type"]).startsWith("application/json")
        assertThat(request.body)
            .contains("\"type\":\"payment.succeeded\"")
            .contains("\"data\":{\"paymentId\":\"pay_123\",\"amount\":4200}")
        assertThat(other.requests).isEmpty()
    }

    @Test
    fun `retries a failing endpoint with exponential backoff until it accepts`() {
        val codes = listOf(500, 503, 200)
        val endpoint = receiver { attempt -> TestReceiver.Reply(codes.getOrElse(attempt - 1) { 200 }) }
        subscribe(endpoint.url, "order.created")

        val delivery = awaitDelivery(publishAccepted("order.created")) { it["status"] == "DELIVERED" }

        assertThat(delivery["attempts"]).isEqualTo(3)
        assertThat(history(delivery).map { it["statusCode"] }).containsExactly(500, 503, 200)
        // The first backoff is 100ms in the test profile, with equal jitter: at least 50ms.
        endpoint.requests.zipWithNext { previous, next ->
            assertThat(Duration.between(previous.receivedAt, next.receivedAt)).isGreaterThanOrEqualTo(Duration.ofMillis(50))
        }
    }

    @Test
    fun `a client error is not retried`() {
        val endpoint = receiver { TestReceiver.Reply(400) }
        subscribe(endpoint.url, "order.cancelled")

        val delivery = awaitDelivery(publishAccepted("order.cancelled")) { it["status"] == "FAILED" }

        assertThat(delivery["attempts"]).isEqualTo(1)
        assertThat(delivery["lastStatusCode"]).isEqualTo(400)
        Thread.sleep(500) // longer than any retry would wait in the test profile
        assertThat(endpoint.requests).hasSize(1)
    }

    @Test
    fun `gives up after the attempt limit, and a manual retry delivers it`() {
        val healthy = AtomicBoolean(false)
        val endpoint = receiver { TestReceiver.Reply(if (healthy.get()) 200 else 500) }
        subscribe(endpoint.url, "invoice.paid")
        val eventId = publishAccepted("invoice.paid")

        val dead = awaitDelivery(eventId) { it["status"] == "DEAD" }
        assertThat(dead["attempts"]).isEqualTo(3) // max-attempts in the test profile
        assertThat(dead["nextAttemptAt"]).isNull()

        healthy.set(true)
        api.post().uri("/deliveries/{id}/retry", dead["id"]).exchange().expectStatus().isAccepted

        val delivered = awaitDelivery(eventId) { it["status"] == "DELIVERED" }
        assertThat(delivered["attempts"]).isEqualTo(4)
        assertThat(history(delivered).map { it["statusCode"] }).containsExactly(500, 500, 500, 200)
        assertThat(endpoint.requests).hasSize(4)
    }

    @Test
    fun `waits as long as the endpoint asks with Retry-After`() {
        val endpoint = receiver { attempt ->
            if (attempt == 1) TestReceiver.Reply(429, mapOf("Retry-After" to "1")) else TestReceiver.Reply(200)
        }
        subscribe(endpoint.url, "report.ready")

        awaitDelivery(publishAccepted("report.ready")) { it["status"] == "DELIVERED" }

        val (first, second) = endpoint.requests
        assertThat(Duration.between(first.receivedAt, second.receivedAt)).isGreaterThanOrEqualTo(Duration.ofMillis(900))
    }

    @Test
    fun `an endpoint that doesn't answer in time is retried`() {
        // response-timeout is 1s in the test profile.
        val endpoint = receiver { attempt ->
            if (attempt == 1) TestReceiver.Reply(200, delay = Duration.ofMillis(1500)) else TestReceiver.Reply(200)
        }
        subscribe(endpoint.url, "statement.generated")

        val delivery = awaitDelivery(publishAccepted("statement.generated")) { it["status"] == "DELIVERED" }

        assertThat(delivery["attempts"]).isEqualTo(2)
        val timedOut = history(delivery).first()
        assertThat(timedOut["statusCode"]).isNull()
        assertThat(timedOut["error"] as String?).isNotBlank()
    }

    @Test
    fun `a slow endpoint doesn't hold up deliveries to the others`() {
        val slow = receiver { TestReceiver.Reply(200, delay = Duration.ofMillis(600)) }
        val fast = receiver()
        subscribe(slow.url, "slow.endpoint")
        subscribe(fast.url, "fast.endpoint")
        repeat(12) { publishAccepted("slow.endpoint") }
        await().atMost(Duration.ofSeconds(10)).until { slow.requests.isNotEmpty() }

        awaitDelivery(publishAccepted("fast.endpoint")) { it["status"] == "DELIVERED" }

        // 12 deliveries, 4 at a time (max-concurrency-per-subscription), 600ms each, take about 1.8s:
        // the fast endpoint's delivery went through while the slow endpoint's backlog was still going.
        assertThat(slow.requests.size).isLessThan(12)
        await().atMost(Duration.ofSeconds(10)).until { slow.requests.size == 12 }
    }

    @Test
    fun `an event no subscription wants is accepted with no deliveries`() {
        val event = publish("nobody.listens").expectStatus().isAccepted
            .expectBody<Map<String, Any?>>().returnResult().responseBody!!

        assertThat(event["deliveries"]).isEqualTo(0)
        assertThat(deliveriesOf(event["id"] as String)).isEmpty()
    }
}
