package io.github.brunostsilva.webhooks

import io.github.brunostsilva.webhooks.delivery.AttemptResult
import io.github.brunostsilva.webhooks.delivery.DeliveryRepository
import io.github.brunostsilva.webhooks.delivery.DeliveryStatus
import io.github.brunostsilva.webhooks.delivery.Transition
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

/** The claim query and the claim token, on their own, with this context's worker off. */
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

    private val noLimit = 1000

    private suspend fun newSubscription(): UUID {
        val subscription = Subscription(
            UUID.randomUUID(), "http://127.0.0.1:9/webhooks", WebhookSigner.newSecret(), listOf("claim.test"), future,
        )
        subscriptions.insert(subscription)
        return subscription.id
    }

    /** Creates `count` deliveries to `subscriptionId`, due at `future`, and returns their events' ids. */
    private suspend fun createDeliveries(subscriptionId: UUID, count: Int): Set<UUID> =
        (1..count).map {
            val event = Event(UUID.randomUUID(), UUID.randomUUID().toString(), "hash", "claim.test", "{}", future)
            events.insertIfAbsent(event)
            deliveries.createAll(event.id, listOf(subscriptionId), future, 3)
            event.id
        }.toSet()

    @Test
    fun `concurrent claims never hand out the same delivery twice`() {
        runBlocking {
            val mine = createDeliveries(newSubscription(), 40)
            val now = future.plusSeconds(1)
            val lease = now.plusSeconds(60)

            val concurrent = (1..8).map {
                async(Dispatchers.IO) { deliveries.claim(now, lease, 10, noLimit) }
            }.awaitAll().flatten()
            val rest = deliveries.claim(now, lease, noLimit, noLimit)

            val claimed = (concurrent + rest).filter { it.eventId in mine }.map { it.deliveryId }
            assertThat(claimed).doesNotHaveDuplicates().hasSize(40)
        }
    }

    @Test
    fun `a delivery whose worker died is claimed again once its lease expires, with a new claim`() {
        runBlocking {
            val mine = createDeliveries(newSubscription(), 1)
            val now = future.plusSeconds(1)

            val first = deliveries.claim(now, now.plusSeconds(30), noLimit, noLimit).filter { it.eventId in mine }
            assertThat(first).hasSize(1)

            // Still leased: no other worker gets it.
            val duringLease = deliveries.claim(now.plusSeconds(10), now.plusSeconds(40), noLimit, noLimit)
            assertThat(duringLease.filter { it.eventId in mine }).isEmpty()

            // The worker never reported back: once the lease is over it's claimed again, attempts unchanged.
            val afterLease = deliveries.claim(now.plusSeconds(31), now.plusSeconds(90), noLimit, noLimit)
                .filter { it.eventId in mine }
            assertThat(afterLease.map { it.deliveryId }).containsExactlyElementsOf(first.map { it.deliveryId })
            assertThat(afterLease.single().attempts).isZero()
            assertThat(afterLease.single().claimId).isNotEqualTo(first.single().claimId)
        }
    }

    @Test
    fun `a worker that lost its claim can't renew the lease, postpone, or record an outcome`() {
        runBlocking {
            val mine = createDeliveries(newSubscription(), 1)
            val now = future.plusSeconds(1)
            val stale = deliveries.claim(now, now.plusSeconds(30), noLimit, noLimit).single { it.eventId in mine }
            val current = deliveries.claim(now.plusSeconds(31), now.plusSeconds(90), noLimit, noLimit)
                .single { it.eventId in mine }
            val later = now.plusSeconds(32)
            val result = AttemptResult(number = 1, statusCode = 200, error = null, duration = Duration.ofMillis(5))

            assertThat(deliveries.renewLease(stale, later.plusSeconds(60), later)).isFalse()
            assertThat(deliveries.postpone(stale, later.plusSeconds(60), later)).isFalse()
            assertThat(deliveries.complete(stale, result, Transition.Delivered, later)).isFalse()
            assertThat(deliveries.attemptsOf(current.deliveryId)).isEmpty()
            assertThat(deliveries.find(current.deliveryId)?.status).isEqualTo(DeliveryStatus.IN_FLIGHT)

            // The worker that holds the claim still can.
            assertThat(deliveries.renewLease(current, later.plusSeconds(60), later)).isTrue()
            assertThat(deliveries.complete(current, result, Transition.Delivered, later)).isTrue()
            assertThat(deliveries.attemptsOf(current.deliveryId)).hasSize(1)
            assertThat(deliveries.find(current.deliveryId)?.status).isEqualTo(DeliveryStatus.DELIVERED)
        }
    }

    @Test
    fun `a claim takes at most the per-subscription limit, and nothing for busy subscriptions`() {
        runBlocking {
            val slow = newSubscription()
            val other = newSubscription()
            createDeliveries(slow, 10)
            createDeliveries(other, 2)
            val now = future.plusSeconds(1)
            val lease = now.plusSeconds(60)

            val first = deliveries.claim(now, lease, noLimit, perSubscription = 3)
            assertThat(first.count { it.subscriptionId == slow }).isEqualTo(3)
            assertThat(first.count { it.subscriptionId == other }).isEqualTo(2)

            // While this instance's slots for `slow` are full, a claim leaves its deliveries alone.
            val whileBusy = deliveries.claim(now, lease, noLimit, perSubscription = 3, busy = listOf(slow))
            assertThat(whileBusy.filter { it.subscriptionId == slow }).isEmpty()

            val next = deliveries.claim(now, lease, noLimit, perSubscription = 3)
            assertThat(next.filter { it.subscriptionId == slow }).hasSize(3)
        }
    }
}
