package io.github.brunostsilva.webhooks.event

import io.github.brunostsilva.webhooks.IdempotencyKeyReusedException
import io.github.brunostsilva.webhooks.InvalidRequestException
import io.github.brunostsilva.webhooks.config.DispatcherProperties
import io.github.brunostsilva.webhooks.delivery.DeliveryRepository
import io.github.brunostsilva.webhooks.subscription.SubscriptionRepository
import java.security.MessageDigest
import java.time.Clock
import java.util.HexFormat
import java.util.UUID
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper

/** The outcome of publishing: the event, how many deliveries it has, and whether it was a replay. */
data class Publication(val event: Event, val deliveries: Int, val replayed: Boolean)

@Service
class EventService(
    private val events: EventRepository,
    private val subscriptions: SubscriptionRepository,
    private val deliveries: DeliveryRepository,
    private val jsonMapper: JsonMapper,
    private val clock: Clock,
    private val properties: DispatcherProperties,
) {

    /**
     * Stores the event and one delivery per matching subscription in the same transaction (the
     * outbox), so an accepted event is never lost and never stored without its deliveries.
     *
     * Publishing is idempotent per `Idempotency-Key`: repeating a request returns the original event
     * without creating anything; reusing the key for a different request is an error.
     */
    @Transactional
    suspend fun publish(idempotencyKey: String, type: String, data: JsonNode): Publication {
        if (idempotencyKey.isBlank() || idempotencyKey.length > MAX_KEY_LENGTH) {
            throw InvalidRequestException("Idempotency-Key must have 1 to $MAX_KEY_LENGTH characters")
        }
        if (type.isBlank() || type.length > MAX_TYPE_LENGTH) {
            throw InvalidRequestException("type must have 1 to $MAX_TYPE_LENGTH characters")
        }

        val requestHash = sha256("$type\n${jsonMapper.writeValueAsString(data)}")
        val now = clock.instant()
        // The webhook body, as the Standard Webhooks spec suggests: type, timestamp and data.
        val payload = jsonMapper.writeValueAsString(
            linkedMapOf("type" to type, "timestamp" to now.toString(), "data" to data),
        )
        val event = Event(UUID.randomUUID(), idempotencyKey, requestHash, type, payload, now)

        if (!events.insertIfAbsent(event)) {
            val original = checkNotNull(events.findByIdempotencyKey(idempotencyKey)) {
                "no event for idempotency key $idempotencyKey"
            }
            if (original.requestHash != requestHash) throw IdempotencyKeyReusedException(idempotencyKey)
            return Publication(original, deliveries.countByEvent(original.id), replayed = true)
        }

        val targets = subscriptions.findByEventType(type)
        deliveries.createAll(event.id, targets.map { it.id }, now, properties.retry.maxAttempts)
        return Publication(event, targets.size, replayed = false)
    }

    private fun sha256(value: String): String =
        HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.toByteArray(Charsets.UTF_8)))

    private companion object {
        const val MAX_KEY_LENGTH = 255
        const val MAX_TYPE_LENGTH = 200
    }
}
