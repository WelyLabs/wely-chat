package com.calendar.chat.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.bind.support.WebExchangeBindException;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;

import java.net.URI;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Turns every failure into an RFC 7807 {@code application/problem+json} response.
 *
 * <p>This service had no error handling at all: an unexpected failure reached the client as
 * Spring's default body, and a missing conversation as an empty 404. Every service now
 * answers in the same RFC 7807 shape, so a client no longer has to know which one it is
 * talking to before it can read an error.
 *
 * <p>Two properties are added beyond the standard fields: {@code code}, the stable
 * identifier a client branches on, and {@code timestamp}, to correlate with logs.
 */
@Slf4j
@RestControllerAdvice
public class GlobalErrorHandler {

    /** Namespace for the {@code type} URI. Not dereferenced; it identifies, it does not serve. */
    private static final String PROBLEM_TYPE_BASE = "https://welylabs.app/problems/";

    @ExceptionHandler(ChatException.class)
    public ProblemDetail handleChatException(ChatException ex, ServerWebExchange exchange) {
        ChatErrorCode error = ex.getErrorCode();

        // Expected outcome, not an incident: logged at WARN, and without a stack trace.
        log.warn("Business failure {} on {}", error.getCode(), exchange.getRequest().getPath());

        return problem(error.getHttpStatus(), error.getCode(), error.getTitle(), error.getDetail(),
                exchange);
    }

    /** Bean Validation rejections, with the offending fields listed. */
    @ExceptionHandler(WebExchangeBindException.class)
    public ProblemDetail handleValidationException(WebExchangeBindException ex,
                                                  ServerWebExchange exchange) {
        Map<String, String> fieldErrors = new LinkedHashMap<>();
        ex.getBindingResult().getFieldErrors()
                .forEach(error -> fieldErrors.put(error.getField(), error.getDefaultMessage()));

        ProblemDetail problem = problem(HttpStatus.BAD_REQUEST, "CHT-VAL-001",
                "Invalid request", "One or more fields are invalid.", exchange);
        problem.setProperty("errors", fieldErrors);

        return problem;
    }

    /**
     * Last resort. The detail is deliberately generic: an exception message can carry a
     * query, a constraint name or a host, and none of that belongs in a response. The real
     * message goes to the logs, where the timestamp ties the two together.
     */
    /**
     * Statuses the framework itself decided: an unknown path, a method that does not apply, a
     * body it cannot read.
     *
     * <p>Without this they fall through to the catch-all below and every one of them answers 500.
     * A missing page reported as a server fault is wrong twice over — it misleads the caller, and
     * it fills the logs with "unhandled failure" for requests that were handled exactly as they
     * should have been.
     */
    @ExceptionHandler(ResponseStatusException.class)
    public ProblemDetail handleResponseStatusException(ResponseStatusException ex,
                                                       ServerWebExchange exchange) {
        HttpStatus status = HttpStatus.valueOf(ex.getStatusCode().value());
        log.debug("{} on {}", status, exchange.getRequest().getPath());

        return problem(status, "CHT-REQ-000", status.getReasonPhrase(),
                "The request could not be served as sent.", exchange);
    }

    @ExceptionHandler(Exception.class)
    public ProblemDetail handleUnexpectedException(Exception ex, ServerWebExchange exchange) {
        log.error("Unhandled failure on {}", exchange.getRequest().getPath(), ex);

        return problem(HttpStatus.INTERNAL_SERVER_ERROR, "CHT-TEC-000",
                "Unexpected error", "The request could not be completed.", exchange);
    }

    private ProblemDetail problem(HttpStatus status, String code, String title, String detail,
                                  ServerWebExchange exchange) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail);
        problem.setType(URI.create(PROBLEM_TYPE_BASE + code.toLowerCase()));
        problem.setTitle(title);
        problem.setInstance(URI.create(exchange.getRequest().getPath().value()));
        problem.setProperty("code", code);
        problem.setProperty("timestamp", Instant.now().toString());

        return problem;
    }
}
