package io.github.brunostsilva.webhooks.event

import java.time.Instant
import java.util.UUID

/** An event published by a producer, with the exact JSON payload its webhooks carry. */
data class Event(
    val id: UUID,
    val idempotencyKey: String,
    val requestHash: String,
    val type: String,
    val payload: String,
    val createdAt: Instant,
)
