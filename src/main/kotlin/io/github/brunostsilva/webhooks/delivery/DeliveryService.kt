package io.github.brunostsilva.webhooks.delivery

import io.github.brunostsilva.webhooks.ConflictException
import io.github.brunostsilva.webhooks.NotFoundException
import io.github.brunostsilva.webhooks.config.DispatcherProperties
import io.github.brunostsilva.webhooks.event.EventRepository
import java.time.Clock
import java.util.UUID
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Isolation
import org.springframework.transaction.annotation.Transactional

/** A delivery and its attempt history. */
data class DeliveryDetails(val delivery: Delivery, val attempts: List<DeliveryAttempt>)

@Service
class DeliveryService(
    private val deliveries: DeliveryRepository,
    private val events: EventRepository,
    private val clock: Clock,
    private val properties: DispatcherProperties,
) {

    /**
     * The event's deliveries and their histories, from one snapshot of the database: with separate
     * statements under READ COMMITTED, a worker finishing an attempt in between could make a delivery
     * show up as DELIVERED after 3 attempts with only 2 of them in its history.
     */
    @Transactional(readOnly = true, isolation = Isolation.REPEATABLE_READ)
    suspend fun ofEvent(eventId: UUID): List<DeliveryDetails> {
        if (events.find(eventId) == null) throw NotFoundException("event", eventId)
        val attempts = deliveries.attemptsByEvent(eventId)
        return deliveries.findByEvent(eventId).map { DeliveryDetails(it, attempts[it.id].orEmpty()) }
    }

    /**
     * Puts a FAILED or DEAD delivery back in the queue, with a fresh budget of attempts. One
     * transaction: the update keeps the row locked until commit (workers skip locked rows), so the
     * delivery returned is exactly the one that was requeued. The caller wakes the worker up once
     * this has returned, i.e. committed: before that, the worker wouldn't see the delivery.
     */
    @Transactional
    suspend fun retry(id: UUID): DeliveryDetails {
        if (!deliveries.retry(id, properties.retry.maxAttempts, clock.instant())) {
            val delivery = deliveries.find(id) ?: throw NotFoundException("delivery", id)
            throw ConflictException("delivery $id is ${delivery.status}: only FAILED or DEAD deliveries can be retried")
        }
        val delivery = checkNotNull(deliveries.find(id)) { "delivery $id disappeared" }
        return DeliveryDetails(delivery, deliveries.attemptsOf(id))
    }
}
