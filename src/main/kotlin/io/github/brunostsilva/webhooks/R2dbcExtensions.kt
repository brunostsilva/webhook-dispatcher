package io.github.brunostsilva.webhooks

import io.r2dbc.spi.Row
import java.time.Instant
import java.util.UUID
import org.springframework.r2dbc.core.DatabaseClient

// Typed reads of non-null columns, and binding of nullable values, for the DatabaseClient repositories.

fun Row.uuid(name: String): UUID = requireNotNull(get(name, UUID::class.java)) { "$name is null" }

fun Row.string(name: String): String = requireNotNull(get(name, String::class.java)) { "$name is null" }

fun Row.int(name: String): Int = requireNotNull(get(name, Int::class.javaObjectType)) { "$name is null" }

fun Row.long(name: String): Long = requireNotNull(get(name, Long::class.javaObjectType)) { "$name is null" }

fun Row.instant(name: String): Instant = requireNotNull(get(name, Instant::class.java)) { "$name is null" }

fun <T : Any> DatabaseClient.GenericExecuteSpec.bindNullable(
    name: String,
    value: T?,
    type: Class<T>,
): DatabaseClient.GenericExecuteSpec = if (value == null) bindNull(name, type) else bind(name, value)
