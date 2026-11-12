package io.github.brunostsilva.webhooks

import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.boot.test.system.CapturedOutput
import org.springframework.boot.test.system.OutputCaptureExtension
import org.springframework.test.context.ActiveProfiles

/**
 * The `json-logs` profile (added to `test`): one ECS JSON object per line, with the events' own fields.
 *
 * Runs in its own JVM (the `jsonLogsTest` Gradle task): logging is configured once per JVM, by the
 * first application context to start, so alongside the other tests it would keep their format.
 */
@ActiveProfiles("json-logs")
@ExtendWith(OutputCaptureExtension::class)
class JsonLogsIntegrationTest : IntegrationTest() {

    @Test
    fun `each line is a JSON object with the level, the message and the event's fields`(output: CapturedOutput) {
        val endpoint = receiver()
        subscribe(endpoint.url, "json.check")
        val eventId = publishAccepted("json.check")
        awaitDelivery(eventId) { it["status"] == "DELIVERED" }

        var line: String? = null
        await().atMost(Duration.ofSeconds(5)).until {
            line = output.out.lines().firstOrNull { it.contains("published with 1 delivery") && it.contains(eventId) }
            line != null
        }

        assertThat(line).startsWith("{").endsWith("}")
        assertThat(line).containsPattern("\"(log\\.)?level\":\"INFO\"")
        assertThat(line).contains("Event $eventId of type json.check published with 1 delivery(ies)")
        // The key-values become fields of their own, e.g. the event type, next to the message.
        assertThat(line).containsPattern("\"(webhook\\.)?event_type\":\"json\\.check\"")
    }
}
