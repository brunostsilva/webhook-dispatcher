package io.github.brunostsilva.webhooks.api

import io.github.brunostsilva.webhooks.config.DispatcherProperties
import java.security.MessageDigest
import org.springframework.core.Ordered
import org.springframework.core.annotation.Order
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.stereotype.Component
import org.springframework.web.server.ServerWebExchange
import org.springframework.web.server.WebFilter
import org.springframework.web.server.WebFilterChain
import reactor.core.publisher.Mono

/**
 * Requires the `X-Api-Key` header on every API call. Actuator endpoints (health probes, metrics) stay
 * open, for the platform to reach them; in production they'd be on a separate, internal port.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
class ApiKeyFilter(properties: DispatcherProperties) : WebFilter {

    private val expected = properties.apiKey.toByteArray(Charsets.UTF_8)

    override fun filter(exchange: ServerWebExchange, chain: WebFilterChain): Mono<Void> {
        val path = exchange.request.path.pathWithinApplication().value()
        if (path == "/actuator" || path.startsWith("/actuator/")) return chain.filter(exchange)

        val provided = exchange.request.headers.getFirst(HEADER)?.toByteArray(Charsets.UTF_8)
        // Constant-time comparison, so response times don't leak how much of the key matched.
        if (provided != null && MessageDigest.isEqual(provided, expected)) return chain.filter(exchange)

        val response = exchange.response
        response.statusCode = HttpStatus.UNAUTHORIZED
        response.headers.contentType = MediaType.APPLICATION_PROBLEM_JSON
        val body = """{"title":"Unauthorized","status":401,"detail":"Missing or invalid $HEADER header"}"""
        return response.writeWith(Mono.just(response.bufferFactory().wrap(body.toByteArray(Charsets.UTF_8))))
    }

    companion object {
        const val HEADER = "X-Api-Key"
    }
}
