package io.github.brunostsilva.webhooks.event

import io.github.brunostsilva.webhooks.instant
import io.github.brunostsilva.webhooks.string
import io.github.brunostsilva.webhooks.uuid
import io.r2dbc.spi.Row
import java.util.UUID
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.stereotype.Repository

@Repository
class EventRepository(private val db: DatabaseClient) {

    /**
     * Stores the event unless its idempotency key is already taken. A concurrent request with the
     * same key waits on the unique index until the first one commits, then inserts nothing.
     */
    suspend fun insertIfAbsent(event: Event): Boolean =
        db.sql(
            """
            insert into events (id, idempotency_key, request_hash, type, payload, created_at)
            values (:id, :idempotencyKey, :requestHash, :type, :payload, :createdAt)
            on conflict (idempotency_key) do nothing
            """,
        )
            .bind("id", event.id)
            .bind("idempotencyKey", event.idempotencyKey)
            .bind("requestHash", event.requestHash)
            .bind("type", event.type)
            .bind("payload", event.payload)
            .bind("createdAt", event.createdAt)
            .fetch().rowsUpdated().awaitSingle() == 1L

    suspend fun find(id: UUID): Event? =
        db.sql("select * from events where id = :id")
            .bind("id", id)
            .map { row, _ -> row.toEvent() }
            .one().awaitSingleOrNull()

    suspend fun findByIdempotencyKey(key: String): Event? =
        db.sql("select * from events where idempotency_key = :key")
            .bind("key", key)
            .map { row, _ -> row.toEvent() }
            .one().awaitSingleOrNull()

    private fun Row.toEvent() = Event(
        id = uuid("id"),
        idempotencyKey = string("idempotency_key"),
        requestHash = string("request_hash"),
        type = string("type"),
        payload = string("payload"),
        createdAt = instant("created_at"),
    )
}
