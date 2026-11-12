package io.github.brunostsilva.webhooks.api

import com.fasterxml.jackson.annotation.JsonInclude
import io.github.brunostsilva.webhooks.delivery.DeliveryDetails
import io.github.brunostsilva.webhooks.delivery.DeliveryStatus
import io.github.brunostsilva.webhooks.event.Publication
import io.github.brunostsilva.webhooks.subscription.Subscription
import java.time.Instant
import java.util.UUID
import tools.jackson.databind.JsonNode

data class CreateSubscriptionRequest(val url: String, val eventTypes: List<String>)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class SubscriptionResponse(
    val id: UUID,
    val url: String,
    val eventTypes: List<String>,
    val createdAt: Instant,
    /** Only returned when the subscription is created. */
    val secret: String? = null,
) {
    companion object {
        fun of(subscription: Subscription, withSecret: Boolean = false) = SubscriptionResponse(
            id = subscription.id,
            url = subscription.url,
            eventTypes = subscription.eventTypes,
            createdAt = subscription.createdAt,
            secret = subscription.secret.takeIf { withSecret },
        )
    }
}

data class PublishEventRequest(val type: String, val data: JsonNode)

data class EventResponse(val id: UUID, val type: String, val createdAt: Instant, val deliveries: Int) {
    companion object {
        fun of(publication: Publication) = EventResponse(
            id = publication.event.id,
            type = publication.event.type,
            createdAt = publication.event.createdAt,
            deliveries = publication.deliveries,
        )
    }
}

data class DeliveryResponse(
    val id: UUID,
    val eventId: UUID,
    val subscriptionId: UUID,
    val status: DeliveryStatus,
    val attempts: Int,
    val attemptLimit: Int,
    val nextAttemptAt: Instant?,
    val lastStatusCode: Int?,
    val lastError: String?,
    val history: List<AttemptResponse>,
) {
    companion object {
        fun of(details: DeliveryDetails): DeliveryResponse {
            val delivery = details.delivery
            val waiting = delivery.status == DeliveryStatus.PENDING || delivery.status == DeliveryStatus.IN_FLIGHT
            return DeliveryResponse(
                id = delivery.id,
                eventId = delivery.eventId,
                subscriptionId = delivery.subscriptionId,
                status = delivery.status,
                attempts = delivery.attempts,
                attemptLimit = delivery.attemptLimit,
                nextAttemptAt = delivery.nextAttemptAt.takeIf { waiting },
                lastStatusCode = delivery.lastStatusCode,
                lastError = delivery.lastError,
                history = details.attempts.map {
                    AttemptResponse(it.attempt, it.statusCode, it.error, it.durationMs, it.attemptedAt)
                },
            )
        }
    }
}

data class AttemptResponse(
    val attempt: Int,
    val statusCode: Int?,
    val error: String?,
    val durationMs: Long,
    val attemptedAt: Instant,
)
