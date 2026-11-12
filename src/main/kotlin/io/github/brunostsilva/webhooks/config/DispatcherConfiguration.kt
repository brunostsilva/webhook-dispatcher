package io.github.brunostsilva.webhooks.config

import io.github.brunostsilva.webhooks.delivery.RetryPolicy
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics
import io.micrometer.core.instrument.MeterRegistry
import io.netty.channel.ChannelOption
import java.time.Clock
import kotlin.random.Random
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.web.reactive.function.client.WebClient
import reactor.netty.http.client.HttpClient

@Configuration(proxyBeanMethods = false)
class DispatcherConfiguration {

    @Bean
    fun clock(): Clock = Clock.systemUTC()

    /** The client that calls subscriber endpoints: bounded timeouts, and redirects are never followed. */
    @Bean
    fun webhookClient(builder: WebClient.Builder, properties: DispatcherProperties): WebClient {
        val httpClient = HttpClient.create()
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, properties.http.connectTimeout.toMillis().toInt())
            .responseTimeout(properties.http.responseTimeout)
            .followRedirect(false)
        return builder.clientConnector(ReactorClientHttpConnector(httpClient)).build()
    }

    /** One circuit breaker per subscription endpoint, all with these settings, exported as metrics. */
    @Bean
    fun circuitBreakerRegistry(properties: DispatcherProperties, meterRegistry: MeterRegistry): CircuitBreakerRegistry {
        val settings = properties.circuitBreaker
        val config = CircuitBreakerConfig.custom()
            .failureRateThreshold(settings.failureRateThreshold)
            .minimumNumberOfCalls(settings.minimumNumberOfCalls)
            .slidingWindowSize(settings.slidingWindowSize)
            .waitDurationInOpenState(settings.waitDurationInOpenState)
            .permittedNumberOfCallsInHalfOpenState(settings.permittedCallsInHalfOpenState)
            .build()
        val registry = CircuitBreakerRegistry.of(config)
        TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(registry).bindTo(meterRegistry)
        return registry
    }

    @Bean
    fun retryPolicy(properties: DispatcherProperties, clock: Clock): RetryPolicy =
        RetryPolicy(properties.retry, clock, Random.Default)
}
