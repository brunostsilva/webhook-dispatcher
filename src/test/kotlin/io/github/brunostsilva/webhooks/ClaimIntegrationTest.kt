package io.github.brunostsilva.webhooks

import io.github.brunostsilva.webhooks.delivery.DeliveryRepository
import io.github.brunostsilva.webhooks.delivery.WebhookSigner
import io.github.brunostsilva.webhooks.event.Event
import io.github.brunostsilva.webhooks.event.EventRepository
import io.github.brunostsilva.webhooks.subscription.Subscription
import io.github.brunostsilva.webhooks.subscription.SubscriptionRepository
import java.time.Duration
import java.time.Instant
import java.util.UUID
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.context.TestPropertySource

/** The claim query on its own, with this context's worker off. */
@TestPropertySource(properties = ["dispatcher.worker.enabled=false"])
class ClaimIntegrationTest : IntegrationTest() {

    @Autowired
    private lateinit var deliveries: DeliveryRepository

    @Autowired
    private lateinit var events: EventRepository

    @Autowired
    private lateinit var subscriptions: SubscriptionRepository

    /** Rows due a day from now, so the workers of other test contexts (on the real clock) leave them alone. */
    private val future = Instant.now().plus(Duration.ofDays(1))

    /** Creates `count` deliveries due at `future` and returns their events' ids. */
    private suspend fun createDeliveries(count: Int): Set<UUID> {
        val subscription = Subscription(
            UUID.randomUUID(), "http://127.0.0.1:9/webhooks", WebhookSigner.newSecret(), listOf("claim.test"), future,
        )
        subscriptions.insert(subscription)
        return (1..count).map {
            val event = Event(UUID.randomUUID(), UUID.randomUUID().toString(), "hash", "claim.test", "{}", future)
            events.insertIfAbsent(event)
            deliveries.createAll(event.id, listOf(subscription.id), future, 3)
            event.id
        }.toSet()
    }

    @Test
    fun `concurrent claims never hand out the same delivery twice`() {
        runBlocking {
            val mine = createDeliveries(40)
            val now = future.plusSeconds(1)
            val lease = now.plusSeconds(60)

            val concurrent = (1..8).map { async(Dispatchers.IO) { deliveries.claim(now, lease, 10) } }.awaitAll().flatten()
            val rest = deliveries.claim(now, lease, 1000)

            val claimed = (concurrent + rest).filter { it.eventId in mine }.map { it.deliveryId }
            assertThat(claimed).doesNotHaveDuplicates().hasSize(40)
        }
    }

    @Test
    fun `a delivery whose worker died is claimed again once its lease expires`() {
        runBlocking {
            val mine = createDeliveries(1)
            val now = future.plusSeconds(1)

            val first = deliveries.claim(now, now.plusSeconds(30), 1000).filter { it.eventId in mine }
            assertThat(first).hasSize(1)

            // Still leased: no other worker gets it.
            val duringLease = deliveries.claim(now.plusSeconds(10), now.plusSeconds(40), 1000)
            assertThat(duringLease.filter { it.eventId in mine }).isEmpty()

            // The worker never reported back: once the lease is over it's claimed again, attempts unchanged.
            val afterLease = deliveries.claim(now.plusSeconds(31), now.plusSeconds(90), 1000).filter { it.eventId in mine }
            assertThat(afterLease.map { it.deliveryId }).containsExactlyElementsOf(first.map { it.deliveryId })
            assertThat(afterLease.single().attempts).isZero()
        }
    }
}
