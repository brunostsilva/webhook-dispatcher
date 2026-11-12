package io.github.brunostsilva.webhooks.delivery

import java.time.Duration
import java.time.Instant
import java.util.UUID

enum class DeliveryStatus {
    /** Waiting for its next attempt. */
    PENDING,

    /** Claimed by a worker, which owns it until its lease expires. */
    IN_FLIGHT,

    /** The endpoint answered 2xx. */
    DELIVERED,

    /** The endpoint refused it with a client error: not retried. */
    FAILED,

    /** Every allowed attempt failed: the dead letter, retried only by hand. */
    DEAD,
}

data class Delivery(
    val id: UUID,
    val eventId: UUID,
    val subscriptionId: UUID,
    val status: DeliveryStatus,
    val attempts: Int,
    val attemptLimit: Int,
    val nextAttemptAt: Instant,
    val lastStatusCode: Int?,
    val lastError: String?,
    val createdAt: Instant,
    val updatedAt: Instant,
)

data class DeliveryAttempt(
    val attempt: Int,
    val statusCode: Int?,
    val error: String?,
    val durationMs: Long,
    val attemptedAt: Instant,
)

/** A delivery claimed by a worker, with everything needed to send it. */
data class DeliveryJob(
    val deliveryId: UUID,
    /** This claim's token: the worker must still hold it to renew the lease or record the outcome. */
    val claimId: UUID,
    val eventId: UUID,
    val subscriptionId: UUID,
    val url: String,
    val secret: String,
    val payload: String,
    val attempts: Int,
    val attemptLimit: Int,
) {
    override fun toString() = "DeliveryJob(deliveryId=$deliveryId, eventId=$eventId, subscriptionId=$subscriptionId)"
}

/** The result of one attempt, as stored in its history. */
data class AttemptResult(val number: Int, val statusCode: Int?, val error: String?, val duration: Duration)

/** Where a delivery goes after an attempt. */
sealed interface Transition {
    val status: DeliveryStatus

    data object Delivered : Transition {
        override val status = DeliveryStatus.DELIVERED
    }

    data object Failed : Transition {
        override val status = DeliveryStatus.FAILED
    }

    data object Dead : Transition {
        override val status = DeliveryStatus.DEAD
    }

    data class Retry(val at: Instant) : Transition {
        override val status = DeliveryStatus.PENDING
    }
}
