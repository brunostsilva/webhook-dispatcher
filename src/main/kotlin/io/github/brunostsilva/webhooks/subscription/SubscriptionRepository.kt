package io.github.brunostsilva.webhooks.subscription

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
class SubscriptionRepository(private val db: DatabaseClient) {

    suspend fun insert(subscription: Subscription) {
        db.sql(
            """
            insert into subscriptions (id, url, secret, event_types, created_at)
            values (:id, :url, :secret, :eventTypes, :createdAt)
            """,
        )
            .bind("id", subscription.id)
            .bind("url", subscription.url)
            .bind("secret", subscription.secret)
            .bind("eventTypes", subscription.eventTypes.toTypedArray())
            .bind("createdAt", subscription.createdAt)
            .fetch().rowsUpdated().awaitSingle()
    }

    suspend fun find(id: UUID): Subscription? =
        db.sql("select * from subscriptions where id = :id")
            .bind("id", id)
            .map { row, _ -> row.toSubscription() }
            .one().awaitSingleOrNull()

    suspend fun findByEventType(type: String): List<Subscription> =
        db.sql("select * from subscriptions where :type = any(event_types) or :all = any(event_types)")
            .bind("type", type)
            .bind("all", Subscription.ALL_EVENTS)
            .map { row, _ -> row.toSubscription() }
            .all().collectList().awaitSingle()

    private fun Row.toSubscription() = Subscription(
        id = uuid("id"),
        url = string("url"),
        secret = string("secret"),
        eventTypes = requireNotNull(get("event_types", Array<String>::class.java)).toList(),
        createdAt = instant("created_at"),
    )
}
