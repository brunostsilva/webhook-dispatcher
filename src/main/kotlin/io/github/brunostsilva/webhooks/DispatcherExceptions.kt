package io.github.brunostsilva.webhooks

import java.util.UUID

/** Errors the API reports to clients, mapped to problem details by `ProblemHandler`. */
sealed class DispatcherException(message: String) : RuntimeException(message)

class InvalidRequestException(message: String) : DispatcherException(message)

class NotFoundException(resource: String, id: UUID) : DispatcherException("$resource $id not found")

class ConflictException(message: String) : DispatcherException(message)

class IdempotencyKeyReusedException(key: String) :
    DispatcherException("Idempotency-Key '$key' was already used with a different request")
