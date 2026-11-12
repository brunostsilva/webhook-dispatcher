package io.github.brunostsilva.webhooks.delivery

import io.github.brunostsilva.webhooks.ConflictException
import io.github.brunostsilva.webhooks.NotFoundException
import io.github.brunostsilva.webhooks.config.DispatcherProperties
import io.github.brunostsilva.webhooks.event.EventRepository
import java.time.Clock
import java.util.UUID
import org.springframework.stereotype.Service

/** A delivery and its attempt history. */
data class DeliveryDetails(val delivery: Delivery, val attempts: List<DeliveryAttempt>)

@Service
class DeliveryService(
    private val deliveries: DeliveryRepository,
    private val events: EventRepository,
    private val signal: WorkerSignal,
    private val clock: Clock,
    private val properties: DispatcherProperties,
) {

    suspend fun ofEvent(eventId: UUID): List<DeliveryDetails> {
        if (events.find(eventId) == null) throw NotFoundException("event", eventId)
        val attempts = deliveries.attemptsByEvent(eventId)
        return deliveries.findByEvent(eventId).map { DeliveryDetails(it, attempts[it.id].orEmpty()) }
    }

    /** Sends a FAILED or DEAD delivery again, with a fresh budget of attempts. */
    suspend fun retry(id: UUID): DeliveryDetails {
        if (!deliveries.retry(id, properties.retry.maxAttempts, clock.instant())) {
            val delivery = deliveries.find(id) ?: throw NotFoundException("delivery", id)
            throw ConflictException("delivery $id is ${delivery.status}: only FAILED or DEAD deliveries can be retried")
        }
        signal.wake()
        val delivery = checkNotNull(deliveries.find(id)) { "delivery $id disappeared" }
        return DeliveryDetails(delivery, deliveries.attemptsOf(id))
    }
}
