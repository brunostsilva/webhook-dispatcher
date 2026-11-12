package io.github.brunostsilva.webhooks.delivery

import java.net.URI
import java.time.Clock
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.awaitExchange

/** The outcome of one HTTP attempt. */
sealed interface WebhookResponse {
    data class Status(val code: Int, val retryAfter: String?) : WebhookResponse

    /** No HTTP response: connection refused, timeout, TLS error... */
    data class Failure(val error: String) : WebhookResponse
}

/** Sends one signed webhook. Never throws for endpoint problems: they come back as a [WebhookResponse]. */
@Component
class WebhookSender(private val webhookClient: WebClient, private val clock: Clock) {

    suspend fun send(job: DeliveryJob): WebhookResponse {
        // The event id is the message id: it's the same on every attempt, so receivers can deduplicate.
        val messageId = job.eventId.toString()
        val timestamp = clock.instant().epochSecond
        return try {
            webhookClient.post()
                .uri(URI.create(job.url))
                .contentType(MediaType.APPLICATION_JSON)
                .header(HttpHeaders.USER_AGENT, USER_AGENT)
                .header("webhook-id", messageId)
                .header("webhook-timestamp", timestamp.toString())
                .header("webhook-signature", WebhookSigner.sign(job.secret, messageId, timestamp, job.payload))
                .bodyValue(job.payload)
                .awaitExchange { response ->
                    response.releaseBody().awaitSingleOrNull()
                    WebhookResponse.Status(
                        code = response.statusCode().value(),
                        retryAfter = response.headers().header(HttpHeaders.RETRY_AFTER).firstOrNull(),
                    )
                }
        } catch (cancellation: CancellationException) {
            throw cancellation
        } catch (error: Exception) {
            WebhookResponse.Failure(error.message ?: error.javaClass.simpleName)
        }
    }

    private companion object {
        const val USER_AGENT = "webhook-dispatcher/1.0"
    }
}
