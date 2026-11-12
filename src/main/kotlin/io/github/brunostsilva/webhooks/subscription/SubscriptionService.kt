package io.github.brunostsilva.webhooks.subscription

import io.github.brunostsilva.webhooks.InvalidRequestException
import io.github.brunostsilva.webhooks.NotFoundException
import io.github.brunostsilva.webhooks.config.DispatcherProperties
import io.github.brunostsilva.webhooks.delivery.WebhookSigner
import java.net.URI
import java.net.URISyntaxException
import java.time.Clock
import java.util.UUID
import org.springframework.stereotype.Service

@Service
class SubscriptionService(
    private val subscriptions: SubscriptionRepository,
    private val clock: Clock,
    private val properties: DispatcherProperties,
) {

    /** Registers an endpoint. The returned subscription carries its signing secret, shown only once. */
    suspend fun create(url: String, eventTypes: List<String>): Subscription {
        validateUrl(url)
        val types = eventTypes.map(String::trim).distinct()
        if (types.isEmpty() || types.any { it.isEmpty() || it.length > MAX_EVENT_TYPE_LENGTH }) {
            throw InvalidRequestException("eventTypes must list 1 or more types of up to $MAX_EVENT_TYPE_LENGTH characters")
        }
        val subscription = Subscription(UUID.randomUUID(), url, WebhookSigner.newSecret(), types, clock.instant())
        subscriptions.insert(subscription)
        return subscription
    }

    suspend fun get(id: UUID): Subscription = subscriptions.find(id) ?: throw NotFoundException("subscription", id)

    private fun validateUrl(url: String) {
        val uri = try {
            URI(url)
        } catch (invalid: URISyntaxException) {
            throw InvalidRequestException("url is not a valid URI: ${invalid.reason}")
        }
        val allowedSchemes = if (properties.allowInsecureUrls) setOf("https", "http") else setOf("https")
        if (uri.scheme?.lowercase() !in allowedSchemes || uri.host.isNullOrBlank()) {
            throw InvalidRequestException("url must be an absolute ${allowedSchemes.joinToString(" or ")} URL")
        }
        if (uri.userInfo != null) throw InvalidRequestException("url must not contain credentials")
    }

    private companion object {
        const val MAX_EVENT_TYPE_LENGTH = 200
    }
}
