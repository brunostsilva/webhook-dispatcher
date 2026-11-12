package io.github.brunostsilva.webhooks.delivery

import io.github.brunostsilva.webhooks.ConflictException
import io.github.brunostsilva.webhooks.NotFoundException
import io.github.brunostsilva.webhooks.config.DispatcherProperties
import io.github.brunostsilva.webhooks.event.EventRepository
import java.time.Clock
import java.util.UUID
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Isolation
import org.springframework.transaction.annotation.Transactional

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

    private val log = LoggerFactory.getLogger(javaClass)

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
     * Sends a FAILED or DEAD delivery again, with a fresh budget of attempts. One transaction: the
     * update keeps the row locked until commit (workers skip locked rows), so the delivery returned is
     * exactly the one that was requeued.
     */
    @Transactional
    suspend fun retry(id: UUID): DeliveryDetails {
        if (!deliveries.retry(id, properties.retry.maxAttempts, clock.instant())) {
            val delivery = deliveries.find(id) ?: throw NotFoundException("delivery", id)
            throw ConflictException("delivery $id is ${delivery.status}: only FAILED or DEAD deliveries can be retried")
        }
        signal.wake()
        val delivery = checkNotNull(deliveries.find(id)) { "delivery $id disappeared" }
        log.atInfo()
            .setMessage("Delivery {} of event {} requeued by hand, {} more attempt(s) allowed")
            .addArgument(id)
            .addArgument(delivery.eventId)
            .addArgument(properties.retry.maxAttempts)
            .addKeyValue("webhook.delivery_id", id)
            .addKeyValue("webhook.event_id", delivery.eventId)
            .addKeyValue("webhook.subscription_id", delivery.subscriptionId)
            .log()
        return DeliveryDetails(delivery, deliveries.attemptsOf(id))
    }
}
