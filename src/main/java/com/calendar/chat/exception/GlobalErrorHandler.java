package com.calendar.chat.exception;

import io.micrometer.core.instrument.MeterRegistry;
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
import java.util.List;
import java.util.Map;

/**
 * Turns every failure into an RFC 7807 {@code application/problem+json} response.
 *
 * <p>This service had no error handling at all: an unexpected failure reached the client as
 * Spring's default body, and a missing conversation as an empty 404. Every service now
 * answers in the same RFC 7807 shape, so a client no longer has to know which one it is
 * talking to before it can read an error.
 *
 * <p>Three properties are added beyond the standard fields: {@code code}, the stable
 * identifier a client branches on, {@code timestamp}, and {@code requestId} — the WebFlux
 * request id that also prefixes the framework's log lines, so a response leads straight to
 * the logs of the request that produced it.
 *
 * <p>Technical failures, and failures nothing expected, are counted in
 * {@value #TECHNICAL_ERRORS} tagged by {@code code}: a dependency going down shows on a
 * dashboard before anyone reads a log.
 */
@Slf4j
@RestControllerAdvice
public class GlobalErrorHandler {

    /** Bean Validation rejected one or more fields. */
    public static final String VALIDATION_FAILED = "CHT-VAL-001";

    /** The framework refused the request itself: unknown path, method not allowed, unreadable body. */
    public static final String REQUEST_REFUSED = "CHT-REQ-000";

    /** A failure nothing anticipated. */
    public static final String UNEXPECTED_FAILURE = "CHT-TEC-000";

    /** Every code this handler writes itself rather than reading from an error-code enum. */
    public static final List<String> HANDLER_CODES = List.of(VALIDATION_FAILED, REQUEST_REFUSED, UNEXPECTED_FAILURE);

    /** Exported by Prometheus as {@code wely_errors_technical_total}. */
    public static final String TECHNICAL_ERRORS = "wely.errors.technical";

    static final String CODE_TAG = "code";

    /** Namespace for the {@code type} URI. Not dereferenced; it identifies, it does not serve. */
    private static final String PROBLEM_TYPE_BASE = "https://welylabs.app/problems/";

    private final MeterRegistry meterRegistry;

    public GlobalErrorHandler(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    @ExceptionHandler(BusinessException.class)
    public ProblemDetail handleBusinessException(BusinessException ex, ServerWebExchange exchange) {
        BusinessErrorCode error = ex.getErrorCode();

        // Expected outcome, not an incident: logged at WARN, and without a stack trace.
        log.warn("Business failure {} on {} (request {})", error.getCode(), exchange.getRequest().getPath(),
                exchange.getRequest().getId());

        return problem(error.getHttpStatus(), error.getCode(), error.getTitle(), error.getDetail(),
                exchange);
    }

    @ExceptionHandler(TechnicalException.class)
    public ProblemDetail handleTechnicalException(TechnicalException ex, ServerWebExchange exchange) {
        TechnicalErrorCode error = ex.getErrorCode();

        // A dependency is down: ERROR with the cause, because someone has to act on it.
        // The previous version logged these at WARN, as business failures, which hid them.
        log.error("Technical failure {} on {} (request {})", error.getCode(), exchange.getRequest().getPath(),
                exchange.getRequest().getId(), ex);
        countTechnicalError(error.getCode());

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

        // The caller's mistake, like a business failure: WARN, without a stack trace.
        log.warn("Validation failure {} on {} (request {})", VALIDATION_FAILED, exchange.getRequest().getPath(),
                exchange.getRequest().getId());

        ProblemDetail problem = problem(HttpStatus.BAD_REQUEST, VALIDATION_FAILED,
                "Invalid request", "One or more fields are invalid.", exchange);
        problem.setProperty("errors", fieldErrors);

        return problem;
    }

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
        log.debug("{} on {} (request {})", status, exchange.getRequest().getPath(), exchange.getRequest().getId());

        return problem(status, REQUEST_REFUSED, status.getReasonPhrase(),
                "The request could not be served as sent.", exchange);
    }

    /**
     * Last resort. The detail is deliberately generic: an exception message can carry a
     * query, a constraint name or a host, and none of that belongs in a response. The real
     * message goes to the logs, where the request id ties the two together.
     */
    @ExceptionHandler(Exception.class)
    public ProblemDetail handleUnexpectedException(Exception ex, ServerWebExchange exchange) {
        log.error("Unhandled failure on {} (request {})", exchange.getRequest().getPath(),
                exchange.getRequest().getId(), ex);
        countTechnicalError(UNEXPECTED_FAILURE);

        return problem(HttpStatus.INTERNAL_SERVER_ERROR, UNEXPECTED_FAILURE,
                "Unexpected error", "The request could not be completed.", exchange);
    }

    private void countTechnicalError(String code) {
        meterRegistry.counter(TECHNICAL_ERRORS, CODE_TAG, code).increment();
    }

    private ProblemDetail problem(HttpStatus status, String code, String title, String detail,
                                  ServerWebExchange exchange) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail);
        problem.setType(URI.create(PROBLEM_TYPE_BASE + code.toLowerCase()));
        problem.setTitle(title);
        problem.setInstance(URI.create(exchange.getRequest().getPath().value()));
        problem.setProperty("code", code);
        problem.setProperty("timestamp", Instant.now().toString());
        problem.setProperty("requestId", exchange.getRequest().getId());

        return problem;
    }
}
