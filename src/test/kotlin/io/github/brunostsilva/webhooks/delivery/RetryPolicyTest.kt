package io.github.brunostsilva.webhooks.delivery

import io.github.brunostsilva.webhooks.config.DispatcherProperties
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import kotlin.math.pow
import kotlin.random.Random
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class RetryPolicyTest {

    private val now = Instant.parse("2026-11-12T10:00:00Z")
    private val settings = DispatcherProperties.Retry(
        maxAttempts = 8,
        initialBackoff = Duration.ofSeconds(10),
        multiplier = 3.0,
        maxBackoff = Duration.ofHours(1),
        maxRetryAfter = Duration.ofHours(6),
    )
    private val policy = RetryPolicy(settings, Clock.fixed(now, ZoneOffset.UTC), Random(42))

    private fun status(code: Int, retryAfter: String? = null) = WebhookResponse.Status(code, retryAfter)

    @Test
    fun `2xx is delivered, client errors are refused, and everything else is retried`() {
        listOf(200, 201, 202, 204).forEach { assertThat(policy.classify(status(it))).isEqualTo(Verdict.Delivered) }
        listOf(301, 302, 400, 401, 403, 404, 410, 422).forEach {
            assertThat(policy.classify(status(it))).isEqualTo(Verdict.Rejected)
        }
        listOf(408, 429, 500, 502, 503, 504).forEach {
            assertThat(policy.classify(status(it))).isEqualTo(Verdict.Retry(retryAfter = null))
        }
        assertThat(policy.classify(WebhookResponse.Failure("Connection refused"))).isEqualTo(Verdict.Retry(retryAfter = null))
    }

    @Test
    fun `backoff grows exponentially, with jitter, up to the cap`() {
        for (attempt in 1..10) {
            val exponential = minOf(10_000 * 3.0.pow(attempt - 1), 3_600_000.0).toLong()
            repeat(100) {
                val delay = policy.backoff(attempt, retryAfter = null).toMillis()
                assertThat(delay).isBetween(exponential / 2, exponential)
            }
        }
    }

    @Test
    fun `Retry-After, in seconds or as an HTTP date, is honoured when longer than the backoff`() {
        val inSeconds = policy.classify(status(503, "120")) as Verdict.Retry
        assertThat(inSeconds.retryAfter).isEqualTo(Duration.ofSeconds(120))
        assertThat(policy.backoff(1, inSeconds.retryAfter)).isEqualTo(Duration.ofSeconds(120))

        val asDate = policy.classify(status(429, "Thu, 12 Nov 2026 10:05:00 GMT")) as Verdict.Retry
        assertThat(asDate.retryAfter).isEqualTo(Duration.ofMinutes(5))

        // A shorter Retry-After doesn't cut the backoff (attempt 5: at least 405s).
        assertThat(policy.backoff(5, Duration.ofSeconds(1))).isGreaterThanOrEqualTo(Duration.ofSeconds(405))
    }

    @Test
    fun `an excessive Retry-After is capped and an invalid one is ignored`() {
        assertThat((policy.classify(status(503, "999999")) as Verdict.Retry).retryAfter).isEqualTo(Duration.ofHours(6))
        assertThat((policy.classify(status(503, "soon")) as Verdict.Retry).retryAfter).isNull()
        assertThat((policy.classify(status(503, "Wed, 11 Nov 2026 10:00:00 GMT")) as Verdict.Retry).retryAfter)
            .isEqualTo(Duration.ZERO)
    }
}
