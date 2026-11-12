package io.github.brunostsilva.webhooks

import io.github.brunostsilva.webhooks.api.ApiKeyFilter
import java.time.Duration
import java.util.UUID
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterEach
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient
import org.springframework.test.web.reactive.server.expectBody
import org.testcontainers.postgresql.PostgreSQLContainer

/**
 * Boots the whole application on a random port, against PostgreSQL in a container (started once and
 * shared by every test class), and talks to it over real HTTP.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
abstract class IntegrationTest {

    @field:Value("\${local.server.port}")
    private var port: Int = 0

    protected val baseUrl: String
        get() = "http://localhost:$port"

    /** A client with the API key. */
    protected val api: WebTestClient by lazy {
        WebTestClient.bindToServer()
            .baseUrl(baseUrl)
            .defaultHeader(ApiKeyFilter.HEADER, API_KEY)
            .responseTimeout(Duration.ofSeconds(15))
            .build()
    }

    private val receivers = mutableListOf<TestReceiver>()

    protected fun receiver(respond: (Int) -> TestReceiver.Reply = { TestReceiver.Reply(200) }): TestReceiver =
        TestReceiver(respond).also(receivers::add)

    @AfterEach
    fun closeReceivers() {
        receivers.forEach(TestReceiver::close)
        receivers.clear()
    }

    protected fun subscribe(url: String, vararg eventTypes: String): Map<String, Any?> =
        api.post().uri("/subscriptions")
            .bodyValue(mapOf("url" to url, "eventTypes" to eventTypes.toList()))
            .exchange()
            .expectStatus().isCreated
            .expectBody<Map<String, Any?>>().returnResult().responseBody!!

    protected fun publish(
        type: String,
        data: Map<String, Any?> = mapOf("id" to UUID.randomUUID().toString()),
        idempotencyKey: String = UUID.randomUUID().toString(),
    ): WebTestClient.ResponseSpec =
        api.post().uri("/events")
            .header("Idempotency-Key", idempotencyKey)
            .bodyValue(mapOf("type" to type, "data" to data))
            .exchange()

    /** Publishes a new event and returns its id. */
    protected fun publishAccepted(type: String): String =
        publish(type).expectStatus().isAccepted
            .expectBody<Map<String, Any?>>().returnResult().responseBody!!["id"] as String

    protected fun deliveriesOf(eventId: String): List<Map<String, Any?>> =
        api.get().uri("/events/{id}/deliveries", eventId)
            .exchange()
            .expectStatus().isOk
            .expectBody<List<Map<String, Any?>>>().returnResult().responseBody!!

    /** Waits until the event's only delivery matches `condition`, and returns it. */
    protected fun awaitDelivery(eventId: String, condition: (Map<String, Any?>) -> Boolean): Map<String, Any?> {
        var match: Map<String, Any?>? = null
        await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(50)).until {
            match = deliveriesOf(eventId).singleOrNull()?.takeIf(condition)
            match != null
        }
        return match!!
    }

    protected fun history(delivery: Map<String, Any?>): List<Map<*, *>> =
        (delivery["history"] as List<*>).map { it as Map<*, *> }

    companion object {
        const val API_KEY = "test-api-key"

        @JvmField
        @ServiceConnection
        val postgres: PostgreSQLContainer = PostgreSQLContainer("postgres:18-alpine").apply { start() }
    }
}
