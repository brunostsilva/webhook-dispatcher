package io.github.brunostsilva.webhooks.subscription

import java.time.Instant
import java.util.UUID

/** An endpoint that receives the events of the given types (`*` for every type). */
data class Subscription(
    val id: UUID,
    val url: String,
    val secret: String,
    val eventTypes: List<String>,
    val createdAt: Instant,
) {
    override fun toString() = "Subscription(id=$id, url=$url, eventTypes=$eventTypes)"

    companion object {
        const val ALL_EVENTS = "*"
    }
}
