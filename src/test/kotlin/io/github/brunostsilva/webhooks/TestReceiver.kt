package io.github.brunostsilva.webhooks

import com.sun.net.httpserver.HttpServer
import java.net.InetAddress
import java.net.InetSocketAddress
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

/**
 * A subscriber endpoint for the tests: a real HTTP server on 127.0.0.1 that records every request
 * and answers the n-th one (from 1) with `respond(n)`.
 */
class TestReceiver(private val respond: (Int) -> Reply) : AutoCloseable {

    data class Reply(val status: Int, val headers: Map<String, String> = emptyMap(), val delay: Duration = Duration.ZERO)

    data class Request(val headers: Map<String, String>, val body: String, val receivedAt: Instant)

    val requests: MutableList<Request> = CopyOnWriteArrayList()
    private val count = AtomicInteger()
    private val server = HttpServer.create(InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0)

    init {
        server.executor = Executors.newVirtualThreadPerTaskExecutor()
        server.createContext("/webhooks") { exchange ->
            exchange.use {
                requests += Request(
                    headers = exchange.requestHeaders.entries.associate { (name, values) -> name.lowercase() to values.first() },
                    body = exchange.requestBody.readAllBytes().toString(Charsets.UTF_8),
                    receivedAt = Instant.now(),
                )
                val reply = respond(count.incrementAndGet())
                if (!reply.delay.isZero) Thread.sleep(reply.delay)
                reply.headers.forEach { (name, value) -> exchange.responseHeaders.add(name, value) }
                exchange.sendResponseHeaders(reply.status, -1)
            }
        }
        server.start()
    }

    val url: String
        get() = "http://${server.address.hostString}:${server.address.port}/webhooks"

    override fun close() = server.stop(0)
}
