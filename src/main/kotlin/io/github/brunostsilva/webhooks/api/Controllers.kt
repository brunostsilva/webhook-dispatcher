package io.github.brunostsilva.webhooks.api

import io.github.brunostsilva.webhooks.delivery.DeliveryService
import io.github.brunostsilva.webhooks.delivery.WorkerSignal
import io.github.brunostsilva.webhooks.event.EventService
import io.github.brunostsilva.webhooks.subscription.SubscriptionService
import java.net.URI
import java.util.UUID
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/subscriptions")
class SubscriptionController(private val subscriptions: SubscriptionService) {

    @PostMapping
    suspend fun create(@RequestBody request: CreateSubscriptionRequest): ResponseEntity<SubscriptionResponse> {
        val subscription = subscriptions.create(request.url, request.eventTypes)
        return ResponseEntity.created(URI.create("/subscriptions/${subscription.id}"))
            .body(SubscriptionResponse.of(subscription, withSecret = true))
    }

    @GetMapping("/{id}")
    suspend fun get(@PathVariable id: UUID): SubscriptionResponse = SubscriptionResponse.of(subscriptions.get(id))
}

@RestController
@RequestMapping("/events")
class EventController(
    private val events: EventService,
    private val deliveries: DeliveryService,
    private val signal: WorkerSignal,
) {

    /**
     * 202 Accepted for a new event, delivered asynchronously; 200 OK with `Idempotent-Replayed: true`
     * when the same request was already accepted under this `Idempotency-Key`.
     */
    @PostMapping
    suspend fun publish(
        @RequestHeader(IDEMPOTENCY_KEY) idempotencyKey: String,
        @RequestBody request: PublishEventRequest,
    ): ResponseEntity<EventResponse> {
        val publication = events.publish(idempotencyKey, request.type, request.data)
        if (!publication.replayed) signal.wake()
        return ResponseEntity.status(if (publication.replayed) HttpStatus.OK else HttpStatus.ACCEPTED)
            .header(IDEMPOTENT_REPLAYED, publication.replayed.toString())
            .body(EventResponse.of(publication))
    }

    @GetMapping("/{id}/deliveries")
    suspend fun deliveries(@PathVariable id: UUID): List<DeliveryResponse> =
        deliveries.ofEvent(id).map(DeliveryResponse::of)

    private companion object {
        const val IDEMPOTENCY_KEY = "Idempotency-Key"
        const val IDEMPOTENT_REPLAYED = "Idempotent-Replayed"
    }
}

@RestController
@RequestMapping("/deliveries")
class DeliveryController(private val deliveries: DeliveryService) {

    /** Sends a FAILED or DEAD delivery again (e.g. once the subscriber fixed their endpoint). */
    @PostMapping("/{id}/retry")
    suspend fun retry(@PathVariable id: UUID): ResponseEntity<DeliveryResponse> =
        ResponseEntity.accepted().body(DeliveryResponse.of(deliveries.retry(id)))
}
