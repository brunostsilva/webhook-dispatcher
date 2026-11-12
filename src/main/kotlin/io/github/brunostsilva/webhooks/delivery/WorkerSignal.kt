package io.github.brunostsilva.webhooks.delivery

import java.time.Duration
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.withTimeoutOrNull
import org.springframework.stereotype.Component

/**
 * Wakes this instance's worker up as soon as an event is stored, instead of waiting for its next
 * poll. Other instances find the event on their next poll.
 */
@Component
class WorkerSignal {

    private val channel = Channel<Unit>(Channel.CONFLATED)

    fun wake() {
        channel.trySend(Unit)
    }

    /** Returns when woken up, or after `timeout`. */
    suspend fun await(timeout: Duration) {
        withTimeoutOrNull(timeout.toMillis()) { channel.receive() }
    }
}
