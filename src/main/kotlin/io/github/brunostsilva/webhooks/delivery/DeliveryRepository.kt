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
     * Claims up to `limit` deliveries that are due, or whose worker's lease expired (it crashed or was
     * killed mid-attempt), and leases them until `leaseUntil`. `FOR UPDATE SKIP LOCKED` lets any number
     * of workers, in any number of instances, claim at the same time without getting the same delivery.
     *
     * Each claim gets a new `claim_id`: only the worker holding it can renew the lease or record the
     * outcome, so a worker whose lease expired can't overwrite the one that took the delivery over.
     *
     * At most `perSubscription` deliveries per subscription are claimed, and none for the `busy`
     * subscriptions (those already sending as much as they're allowed to in this instance), so what's
     * claimed can start right away and a slow endpoint's backlog doesn't crowd out the others.
     */
    suspend fun claim(
        now: Instant,
        leaseUntil: Instant,
        limit: Int,
        perSubscription: Int,
        busy: Collection<UUID> = emptyList(),
    ): List<DeliveryJob> =
        db.sql(
            """
            with candidates as (
                select id, subscription_id, next_attempt_at from deliveries
                where ((status = 'PENDING' and next_attempt_at <= :now) or (status = 'IN_FLIGHT' and lease_until < :now))
                  and subscription_id::text <> all(:busy)
                order by next_attempt_at
                limit :scan
                for update skip locked
            ),
            picked as (
                select id from (
                    select id, next_attempt_at,
                           row_number() over (partition by subscription_id order by next_attempt_at) as position
                    from candidates
                ) ranked
                where position <= :perSubscription
                order by next_attempt_at
                limit :limit
            ),
            claimed as (
                update deliveries d
                set status = 'IN_FLIGHT', lease_until = :leaseUntil, claim_id = gen_random_uuid(), updated_at = :now
                from picked
                where d.id = picked.id
                returning d.id, d.event_id, d.subscription_id, d.attempts, d.attempt_limit, d.claim_id
            )
            select c.id, c.event_id, c.subscription_id, c.attempts, c.attempt_limit, c.claim_id, e.payload, s.url, s.secret
            from claimed c
            join events e on e.id = c.event_id
            join subscriptions s on s.id = c.subscription_id
            """,
        )
            .bind("now", now)
            .bind("leaseUntil", leaseUntil)
            .bind("limit", limit)
            .bind("scan", limit * SCAN_FACTOR)
            .bind("perSubscription", perSubscription)
            .bind("busy", busy.map(UUID::toString).toTypedArray())
            .map { row, _ ->
                DeliveryJob(
                    deliveryId = row.uuid("id"),
                    claimId = row.uuid("claim_id"),
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

    /**
     * Extends the lease of a claimed delivery right before it's sent. False if the claim was lost (the
     * lease expired and another worker took the delivery over): it must not be sent then.
     */
    suspend fun renewLease(job: DeliveryJob, leaseUntil: Instant, now: Instant): Boolean =
        db.sql(
            """
            update deliveries set lease_until = :leaseUntil, updated_at = :now
            where id = :id and claim_id = :claimId and status = 'IN_FLIGHT'
            """,
        )
            .bind("id", job.deliveryId)
            .bind("claimId", job.claimId)
            .bind("leaseUntil", leaseUntil)
            .bind("now", now)
            .fetch().rowsUpdated().awaitSingle() == 1L

    /** Gives a claimed delivery back without attempting it, e.g. while its endpoint's circuit is open. */
    suspend fun postpone(job: DeliveryJob, until: Instant, now: Instant): Boolean =
        db.sql(
            """
            update deliveries
            set status = 'PENDING', next_attempt_at = :until, lease_until = null, claim_id = null, updated_at = :now
            where id = :id and claim_id = :claimId and status = 'IN_FLIGHT'
            """,
        )
            .bind("id", job.deliveryId)
            .bind("claimId", job.claimId)
            .bind("until", until)
            .bind("now", now)
            .fetch().rowsUpdated().awaitSingle() == 1L

    /**
     * Moves the delivery on and records the attempt in its history, atomically, if this worker still
     * holds the claim. False (and nothing recorded) if the claim was lost in the meantime.
     */
    @Transactional
    suspend fun complete(job: DeliveryJob, attempt: AttemptResult, transition: Transition, now: Instant): Boolean {
        val nextAttemptAt = if (transition is Transition.Retry) transition.at else now
        val updated = db.sql(
            """
            update deliveries
            set status = :status, attempts = :attempts, next_attempt_at = :nextAttemptAt, lease_until = null,
                claim_id = null, last_status_code = :statusCode, last_error = :error, updated_at = :now
            where id = :id and claim_id = :claimId and status = 'IN_FLIGHT'
            """,
        )
            .bind("id", job.deliveryId)
            .bind("claimId", job.claimId)
            .bind("status", transition.status.name)
            .bind("attempts", attempt.number)
            .bind("nextAttemptAt", nextAttemptAt)
            .bindNullable("statusCode", attempt.statusCode, Int::class.javaObjectType)
            .bindNullable("error", attempt.error, String::class.java)
            .bind("now", now)
            .fetch().rowsUpdated().awaitSingle()
        if (updated != 1L) return false

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
        return true
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

    private companion object {
        /** Candidate rows looked at per delivery claimed, so the per-subscription cap still fills a claim. */
        const val SCAN_FACTOR = 4
    }
}
