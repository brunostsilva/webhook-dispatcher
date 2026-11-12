package io.github.brunostsilva.webhooks

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.async.AsyncLoggerContext
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.springframework.boot.logging.LoggingSystem
import org.springframework.boot.logging.log4j2.Log4J2LoggingSystem

/** Guards the logging setup: SLF4J on Log4j2 (not Logback), driven by Spring Boot, with async loggers. */
class LoggingBackendTest {

    @Test
    fun `SLF4J is backed by Log4j2`() {
        assertThat(LoggerFactory.getILoggerFactory().javaClass.name).startsWith("org.apache.logging.slf4j")
    }

    @Test
    fun `Spring Boot uses the Log4j2 logging system`() {
        assertThat(LoggingSystem.get(javaClass.classLoader)).isInstanceOf(Log4J2LoggingSystem::class.java)
    }

    @Test
    fun `every logger is asynchronous, so logging never blocks the caller on I-O`() {
        assertThat(LogManager.getContext(false)).isInstanceOf(AsyncLoggerContext::class.java)
    }
}
