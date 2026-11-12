package io.github.brunostsilva.webhooks.api

import io.github.brunostsilva.webhooks.ConflictException
import io.github.brunostsilva.webhooks.DispatcherException
import io.github.brunostsilva.webhooks.IdempotencyKeyReusedException
import io.github.brunostsilva.webhooks.InvalidRequestException
import io.github.brunostsilva.webhooks.NotFoundException
import org.springframework.http.HttpStatusCode
import org.springframework.http.MediaType
import org.springframework.http.ProblemDetail
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice

/** Errors as RFC 9457 problem details (`application/problem+json`). */
@RestControllerAdvice
class ProblemHandler {

    @ExceptionHandler
    fun handle(error: DispatcherException): ResponseEntity<ProblemDetail> {
        val (status, title) = when (error) {
            is InvalidRequestException -> 400 to "Invalid request"
            is NotFoundException -> 404 to "Not found"
            is ConflictException -> 409 to "Conflict"
            is IdempotencyKeyReusedException -> 422 to "Idempotency key reused"
        }
        val problem = ProblemDetail.forStatusAndDetail(HttpStatusCode.valueOf(status), error.message)
        problem.title = title
        return ResponseEntity.status(status).contentType(MediaType.APPLICATION_PROBLEM_JSON).body(problem)
    }
}
