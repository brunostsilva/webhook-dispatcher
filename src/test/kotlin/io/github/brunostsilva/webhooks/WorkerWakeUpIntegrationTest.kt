package io.github.brunostsilva.webhooks

import io.github.brunostsilva.webhooks.delivery.WorkerSignal
import java.sql.DriverManager
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.context.annotation.Primary

/**
 * Waking the worker up before the change is committed is useless: it claims, finds nothing, and the
 * delivery waits for the next poll. This records, at each wake-up, what a separate connection (which
 * only sees committed data) reads.
 */
@Import(WorkerWakeUpIntegrationTest.RecordingSignalConfiguration::class)
class WorkerWakeUpIntegrationTest : IntegrationTest() {

    /** Calls `onWake` before each wake-up. */
    class RecordingWorkerSignal : WorkerSignal() {
        @Volatile
        var onWake: (() -> Unit)? = null

        override fun wake() {
            onWake?.invoke()
            super.wake()
        }
    }

    @TestConfiguration
    class RecordingSignalConfiguration {
        @Bean
        @Primary
        fun recordingWorkerSignal() = RecordingWorkerSignal()
    }

    @Autowired
    private lateinit var signal: RecordingWorkerSignal

    /** The committed status of a delivery, read over its own JDBC connection. */
    private fun committedStatus(deliveryId: String): String =
        DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password).use { connection ->
            connection.prepareStatement("select status from deliveries where id = ?::uuid").use { statement ->
                statement.setString(1, deliveryId)
                statement.executeQuery().use { rows -> if (rows.next()) rows.getString(1) else "MISSING" }
            }
        }

    @Test
    fun `a manual retry wakes the worker up only once the requeue is committed`() {
        val accepting = AtomicBoolean(false)
        val endpoint = receiver { TestReceiver.Reply(if (accepting.get()) 200 else 400) }
        subscribe(endpoint.url, "wake.check")
        val failed = awaitDelivery(publishAccepted("wake.check")) { it["status"] == "FAILED" }
        val deliveryId = failed["id"] as String
        Thread.sleep(300) // let the worker's own wake-up after that attempt go by
        accepting.set(true) // from now on it can only become PENDING, IN_FLIGHT or DELIVERED

        val statusesAtWakeUp = CopyOnWriteArrayList<String>()
        signal.onWake = { statusesAtWakeUp += committedStatus(deliveryId) }
        try {
            api.post().uri("/deliveries/{id}/retry", deliveryId).exchange().expectStatus().isAccepted
        } finally {
            signal.onWake = null
        }

        assertThat(statusesAtWakeUp).isNotEmpty().doesNotContain("FAILED")
    }
}
