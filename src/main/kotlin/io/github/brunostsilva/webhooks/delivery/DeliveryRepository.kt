package io.github.brunostsilva.webhooks.delivery

import io.github.brunostsilva.webhooks.bindNullable
import io.github.brunostsilva.webhooks.instant
import io.github.brunostsilva.webhooks.int
import io.github.brunostsilva.webhooks.long
import io.github.brunostsilva.webhooks.string
import io.github.brunostsilva.webhooks.uuid
import io.r2dbc.spi.Row
import java.time.Instant
import java.util.UUID
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional

@Repository
class DeliveryRepository(private val db: DatabaseClient) {

    /** One PENDING delivery per subscription, due now. Runs inside the caller's transaction. */
    suspend fun createAll(eventId: UUID, subscriptionIds: List<UUID>, now: Instant, attemptLimit: Int) {
        for (subscriptionId in subscriptionIds) {
            db.sql(
                """
                insert into deliveries
                    (id, event_id, subscription_id, status, attempts, attempt_limit, next_attempt_at, created_at, updated_at)
                values (:id, :eventId, :subscriptionId, 'PENDING', 0, :attemptLimit, :now, :now, :now)
                """,
            )
                .bind("id", UUID.randomUUID())
                .bind("eventId", eventId)
                .bind("subscriptionId", subscriptionId)
                .bind("attemptLimit", attemptLimit)
                .bind("now", now)
                .fetch().rowsUpdated().awaitSingle()
        }
    }

    /**
     * Claims up to `limit` deliveries that are due, or whose worker's lease expired (it crashed or
     * was killed mid-attempt), and leases them until `leaseUntil`. `FOR UPDATE SKIP LOCKED` lets any
     * number of workers, in any number of instances, claim at the same time without ever getting the
     * same delivery.
     */
    suspend fun claim(now: Instant, leaseUntil: Instant, limit: Int): List<DeliveryJob> =
        db.sql(
            """
            with claimed as (
                update deliveries
                set status = 'IN_FLIGHT', lease_until = :leaseUntil, updated_at = :now
                where id in (
                    select id from deliveries
                    where (status = 'PENDING' and next_attempt_at <= :now)
                       or (status = 'IN_FLIGHT' and lease_until < :now)
                    order by next_attempt_at
                    limit :limit
                    for update skip locked
                )
                returning id, event_id, subscription_id, attempts, attempt_limit
            )
            select c.id, c.event_id, c.subscription_id, c.attempts, c.attempt_limit, e.payload, s.url, s.secret
            from claimed c
            join events e on e.id = c.event_id
            join subscriptions s on s.id = c.subscription_id
            """,
        )
            .bind("now", now)
            .bind("leaseUntil", leaseUntil)
            .bind("limit", limit)
            .map { row, _ ->
                DeliveryJob(
                    deliveryId = row.uuid("id"),
                    eventId = row.uuid("event_id"),
                    subscriptionId = row.uuid("subscription_id"),
                    url = row.string("url"),
                    secret = row.string("secret"),
                    payload = row.string("payload"),
                    attempts = row.int("attempts"),
                    attemptLimit = row.int("attempt_limit"),
                )
            }
            .all().collectList().awaitSingle()

    /** Gives a claimed delivery back without attempting it, e.g. while its endpoint's circuit is open. */
    suspend fun postpone(id: UUID, until: Instant, now: Instant) {
        db.sql(
            """
            update deliveries set status = 'PENDING', next_attempt_at = :until, lease_until = null, updated_at = :now
            where id = :id and status = 'IN_FLIGHT'
            """,
        )
            .bind("id", id)
            .bind("until", until)
            .bind("now", now)
            .fetch().rowsUpdated().awaitSingle()
    }

    /** Records an attempt in the history and moves the delivery on, atomically. */
    @Transactional
    suspend fun complete(job: DeliveryJob, attempt: AttemptResult, transition: Transition, now: Instant) {
        db.sql(
            """
            insert into delivery_attempts (delivery_id, attempt, status_code, error, duration_ms, attempted_at)
            values (:deliveryId, :attempt, :statusCode, :error, :durationMs, :now)
            """,
        )
            .bind("deliveryId", job.deliveryId)
            .bind("attempt", attempt.number)
            .bindNullable("statusCode", attempt.statusCode, Int::class.javaObjectType)
            .bindNullable("error", attempt.error, String::class.java)
            .bind("durationMs", attempt.duration.toMillis())
            .bind("now", now)
            .fetch().rowsUpdated().awaitSingle()

        val nextAttemptAt = if (transition is Transition.Retry) transition.at else now
        db.sql(
            """
            update deliveries
            set status = :status, attempts = :attempts, next_attempt_at = :nextAttemptAt, lease_until = null,
                last_status_code = :statusCode, last_error = :error, updated_at = :now
            where id = :id and status = 'IN_FLIGHT'
            """,
        )
            .bind("id", job.deliveryId)
            .bind("status", transition.status.name)
            .bind("attempts", attempt.number)
            .bind("nextAttemptAt", nextAttemptAt)
            .bindNullable("statusCode", attempt.statusCode, Int::class.javaObjectType)
            .bindNullable("error", attempt.error, String::class.java)
            .bind("now", now)
            .fetch().rowsUpdated().awaitSingle()
    }

    /** Puts a FAILED or DEAD delivery back in the queue with a new budget of attempts. */
    suspend fun retry(id: UUID, extraAttempts: Int, now: Instant): Boolean =
        db.sql(
            """
            update deliveries
            set status = 'PENDING', next_attempt_at = :now, attempt_limit = attempts + :extraAttempts, updated_at = :now
            where id = :id and status in ('FAILED', 'DEAD')
            """,
        )
            .bind("id", id)
            .bind("extraAttempts", extraAttempts)
            .bind("now", now)
            .fetch().rowsUpdated().awaitSingle() == 1L

    suspend fun find(id: UUID): Delivery? =
        db.sql("select * from deliveries where id = :id")
            .bind("id", id)
            .map { row, _ -> row.toDelivery() }
            .one().awaitSingleOrNull()

    suspend fun findByEvent(eventId: UUID): List<Delivery> =
        db.sql("select * from deliveries where event_id = :eventId order by created_at, id")
            .bind("eventId", eventId)
            .map { row, _ -> row.toDelivery() }
            .all().collectList().awaitSingle()

    suspend fun countByEvent(eventId: UUID): Int =
        db.sql("select count(*) as total from deliveries where event_id = :eventId")
            .bind("eventId", eventId)
            .map { row, _ -> row.long("total").toInt() }
            .one().awaitSingle()

    /** The attempt history of the deliveries of an event, by delivery, oldest attempt first. */
    suspend fun attemptsByEvent(eventId: UUID): Map<UUID, List<DeliveryAttempt>> =
        db.sql(
            """
            select a.* from delivery_attempts a
            join deliveries d on d.id = a.delivery_id
            where d.event_id = :eventId
            order by a.delivery_id, a.attempt
            """,
        )
            .bind("eventId", eventId)
            .map { row, _ -> row.uuid("delivery_id") to row.toAttempt() }
            .all().collectList().awaitSingle()
            .groupBy({ it.first }, { it.second })

    suspend fun attemptsOf(deliveryId: UUID): List<DeliveryAttempt> =
        db.sql("select * from delivery_attempts where delivery_id = :deliveryId order by attempt")
            .bind("deliveryId", deliveryId)
            .map { row, _ -> row.toAttempt() }
            .all().collectList().awaitSingle()

    private fun Row.toDelivery() = Delivery(
        id = uuid("id"),
        eventId = uuid("event_id"),
        subscriptionId = uuid("subscription_id"),
        status = DeliveryStatus.valueOf(string("status")),
        attempts = int("attempts"),
        attemptLimit = int("attempt_limit"),
        nextAttemptAt = instant("next_attempt_at"),
        lastStatusCode = get("last_status_code", Int::class.javaObjectType),
        lastError = get("last_error", String::class.java),
        createdAt = instant("created_at"),
        updatedAt = instant("updated_at"),
    )

    private fun Row.toAttempt() = DeliveryAttempt(
        attempt = int("attempt"),
        statusCode = get("status_code", Int::class.javaObjectType),
        error = get("error", String::class.java),
        durationMs = long("duration_ms"),
        attemptedAt = instant("attempted_at"),
    )
}
