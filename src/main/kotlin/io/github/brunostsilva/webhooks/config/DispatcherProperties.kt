package io.github.brunostsilva.webhooks.config

import java.time.Duration
import org.springframework.boot.context.properties.ConfigurationProperties

/** Settings under `dispatcher.*`; see application.yaml for the production defaults. */
@ConfigurationProperties("dispatcher")
data class DispatcherProperties(
    val apiKey: String,
    val allowInsecureUrls: Boolean = false,
    val worker: Worker = Worker(),
    val retry: Retry = Retry(),
    val http: Http = Http(),
    val circuitBreaker: CircuitBreaker = CircuitBreaker(),
) {
    init {
        require(apiKey.isNotBlank()) { "dispatcher.api-key must be set" }
        // The lease is renewed when an attempt starts, and must outlast it.
        require(worker.lease > http.connectTimeout + http.responseTimeout) {
            "dispatcher.worker.lease must be longer than dispatcher.http.connect-timeout + response-timeout"
        }
    }

    data class Worker(
        val enabled: Boolean = true,
        val pollInterval: Duration = Duration.ofSeconds(1),
        val batchSize: Int = 50,
        val lease: Duration = Duration.ofSeconds(60),
        val maxConcurrency: Int = 64,
        val maxConcurrencyPerSubscription: Int = 4,
        val shutdownTimeout: Duration = Duration.ofSeconds(20),
    ) {
        init {
            require(batchSize > 0 && maxConcurrency > 0 && maxConcurrencyPerSubscription > 0) {
                "dispatcher.worker sizes must be positive"
            }
        }
    }

    data class Retry(
        val maxAttempts: Int = 8,
        val initialBackoff: Duration = Duration.ofSeconds(10),
        val multiplier: Double = 3.0,
        val maxBackoff: Duration = Duration.ofHours(1),
        /** Longest wait accepted from an endpoint's `Retry-After` header. */
        val maxRetryAfter: Duration = Duration.ofHours(6),
    ) {
        init {
            require(maxAttempts > 0) { "dispatcher.retry.max-attempts must be positive" }
            require(multiplier >= 1.0) { "dispatcher.retry.multiplier must be at least 1" }
        }
    }

    data class Http(
        val connectTimeout: Duration = Duration.ofSeconds(3),
        val responseTimeout: Duration = Duration.ofSeconds(10),
    )

    data class CircuitBreaker(
        val failureRateThreshold: Float = 50f,
        val minimumNumberOfCalls: Int = 10,
        val slidingWindowSize: Int = 20,
        val waitDurationInOpenState: Duration = Duration.ofSeconds(60),
        val permittedCallsInHalfOpenState: Int = 2,
    )
}
