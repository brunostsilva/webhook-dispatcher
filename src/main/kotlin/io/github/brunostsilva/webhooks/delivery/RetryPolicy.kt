package io.github.brunostsilva.webhooks.delivery

import io.github.brunostsilva.webhooks.config.DispatcherProperties
import java.time.Clock
import java.time.Duration
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import kotlin.math.pow
import kotlin.random.Random

/** What to do after an attempt. */
sealed interface Verdict {
    /** 2xx: the endpoint took it. */
    data object Delivered : Verdict

    /** A client error other than 408 and 429: the endpoint refused it, and retrying won't change that. */
    data object Rejected : Verdict

    /** A network error, a timeout, 408, 429 or 5xx: worth trying again, after `retryAfter` if the endpoint said so. */
    data class Retry(val retryAfter: Duration?) : Verdict
}

class RetryPolicy(
    private val settings: DispatcherProperties.Retry,
    private val clock: Clock,
    private val random: Random,
) {

    fun classify(response: WebhookResponse): Verdict = when (response) {
        is WebhookResponse.Failure -> Verdict.Retry(retryAfter = null)
        is WebhookResponse.Status -> when (response.code) {
            in 200..299 -> Verdict.Delivered
            408, 429 -> Verdict.Retry(retryAfter(response.retryAfter))
            in 500..599 -> Verdict.Retry(retryAfter(response.retryAfter))
            else -> Verdict.Rejected // includes 3xx: redirects are never followed
        }
    }

    /**
     * Delay before attempt `attempt + 1`: exponential (`initialBackoff * multiplier^(attempt-1)`, capped
     * at `maxBackoff`) with "equal jitter" (half fixed, half random), so endpoints that failed together
     * aren't all retried at the same instant. An endpoint's own `Retry-After` wins when it's longer.
     */
    fun backoff(attempt: Int, retryAfter: Duration?): Duration {
        val exponential = settings.initialBackoff.toMillis() * settings.multiplier.pow(attempt - 1)
        val capped = minOf(exponential, settings.maxBackoff.toMillis().toDouble()).toLong()
        val jittered = capped / 2 + random.nextLong(capped / 2 + 1)
        val delay = Duration.ofMillis(jittered)
        return if (retryAfter != null && retryAfter > delay) retryAfter else delay
    }

    /** `Retry-After` in seconds or as an HTTP date, capped at `maxRetryAfter`; ignored when invalid. */
    private fun retryAfter(header: String?): Duration? {
        if (header.isNullOrBlank()) return null
        val delay = header.trim().toLongOrNull()?.let(Duration::ofSeconds)
            ?: try {
                Duration.between(clock.instant(), ZonedDateTime.parse(header.trim(), DateTimeFormatter.RFC_1123_DATE_TIME))
            } catch (invalid: DateTimeParseException) {
                return null
            }
        return when {
            delay.isNegative -> Duration.ZERO
            delay > settings.maxRetryAfter -> settings.maxRetryAfter
            else -> delay
        }
    }
}
