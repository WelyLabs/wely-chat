package com.calendar.chat.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.bind.support.WebExchangeBindException;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Turns every failure into an RFC 7807 {@code application/problem+json} response, built by
 * {@link ProblemDetails#of}.
 *
 * <p>This service had no error handling at all: an unexpected failure reached the client as
 * Spring's default body, and a missing conversation as an empty 404. Every service now
 * answers in the same RFC 7807 shape, so a client no longer has to know which one it is
 * talking to before it can read an error.
 *
 * <p>Ordered first, so no other advice — the framework's own {@code ResponseEntityExceptionHandler}
 * included, should it ever be registered — answers an exception this one handles in a shape
 * without {@code code} or {@code requestId}.
 *
 * <p>Technical failures, and failures nothing expected, are counted in
 * {@value #TECHNICAL_ERRORS_METRIC} tagged by {@code code} and {@code source} (here {@value #SOURCE_HTTP}),
 * through {@link TechnicalErrorCounter}: a dependency going down shows on a dashboard before anyone
 * reads a log.
 */
@Slf4j
@Order(Ordered.HIGHEST_PRECEDENCE)
@RestControllerAdvice
public class GlobalErrorHandler {

    /** Bean Validation rejected one or more fields. */
    public static final String VALIDATION_FAILED_CODE = "CHT-VAL-001";

    /** The framework refused the request itself: unknown path, method not allowed, unreadable body. */
    public static final String REQUEST_REFUSED_CODE = "CHT-REQ-000";

    /** A failure nothing anticipated. */
    public static final String UNEXPECTED_FAILURE_CODE = "CHT-TEC-000";

    /** Every code this handler writes itself rather than reading from an error-code enum. */
    public static final List<String> HANDLER_CODES =
            List.of(VALIDATION_FAILED_CODE, REQUEST_REFUSED_CODE, UNEXPECTED_FAILURE_CODE);

    /** Exported by Prometheus as {@code wely_errors_technical_total}. */
    public static final String TECHNICAL_ERRORS_METRIC = "wely.errors.technical";

    public static final String CODE_TAG = "code";

    /** Where a technical failure surfaced; see {@link TechnicalErrorCounter}. */
    public static final String SOURCE_TAG = "source";

    /** Answered to an HTTP request: the advice, the WebExceptionHandler, the security writers. */
    public static final String SOURCE_HTTP = "http";

    /** Raised while consuming a Kafka record. */
    public static final String SOURCE_KAFKA = "kafka";

    /** Raised by an outbox relay or dispatcher. This service has none; kept for one vocabulary across services. */
    public static final String SOURCE_OUTBOX = "outbox";

    /** Raised by the deleted-users reconciliation loop. */
    public static final String SOURCE_RECONCILIATION = "reconciliation";

    /** Raised by a scheduled job: call reaper, presence heartbeat, stream sweep. */
    public static final String SOURCE_JOB = "job";

    /** Answered to an RSocket route, as an error frame. */
    public static final String SOURCE_RSOCKET = "rsocket";

    private final TechnicalErrorCounter counter;

    public GlobalErrorHandler(TechnicalErrorCounter counter) {
        this.counter = counter;
    }

    /**
     * The same mapping as the handlers below, for failures no controller advice sees — raised in a
     * {@code WebFilter} or before a handler is chosen. See {@link ProblemWebExceptionHandler}.
     */
    public ProblemDetail handle(Throwable failure, ServerWebExchange exchange) {
        return switch (failure) {
            case BusinessException business -> handleBusinessException(business, exchange);
            case TechnicalException technical -> handleTechnicalException(technical, exchange);
            case WebExchangeBindException invalid -> handleValidationException(invalid, exchange);
            case ResponseStatusException refused -> handleResponseStatusException(refused, exchange);
            case Exception unexpected -> handleUnexpectedException(unexpected, exchange);
            default -> unexpected(failure, exchange);
        };
    }

    @ExceptionHandler(BusinessException.class)
    public ProblemDetail handleBusinessException(BusinessException ex, ServerWebExchange exchange) {
        BusinessErrorCode error = ex.getErrorCode();

        // Expected outcome, not an incident: logged at WARN, and without a stack trace.
        log.warn("Business failure {} on {} (request {})", error.getCode(), exchange.getRequest().getPath(),
                RequestIds.of(exchange));

        return ProblemDetails.of(error, exchange);
    }

    @ExceptionHandler(TechnicalException.class)
    public ProblemDetail handleTechnicalException(TechnicalException ex, ServerWebExchange exchange) {
        TechnicalErrorCode error = ex.getErrorCode();

        // A dependency is down: ERROR with the cause, because someone has to act on it.
        // The previous version logged these at WARN, as business failures, which hid them.
        log.error("Technical failure {} on {} (request {})", error.getCode(), exchange.getRequest().getPath(),
                RequestIds.of(exchange), ex);
        counter.count(ex, SOURCE_HTTP);

        return ProblemDetails.of(error.getHttpStatus(), error.getCode(), error.getTitle(), error.getDetail(),
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
        log.warn("Validation failure {} on {} (request {})", VALIDATION_FAILED_CODE,
                exchange.getRequest().getPath(), RequestIds.of(exchange));

        ProblemDetail problem = ProblemDetails.of(HttpStatus.BAD_REQUEST, VALIDATION_FAILED_CODE,
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
        HttpStatusCode status = ex.getStatusCode();
        String reason = status instanceof HttpStatus known ? known.getReasonPhrase() : "Request refused";
        log.debug("{} on {} (request {})", status, exchange.getRequest().getPath(), RequestIds.of(exchange));

        return ProblemDetails.of(status, REQUEST_REFUSED_CODE, reason,
                "The request could not be served as sent.", exchange);
    }

    /**
     * Last resort. The detail is deliberately generic: an exception message can carry a
     * query, a constraint name or a host, and none of that belongs in a response. The real
     * message goes to the logs, where the request id ties the two together.
     */
    @ExceptionHandler(Exception.class)
    public ProblemDetail handleUnexpectedException(Exception ex, ServerWebExchange exchange) {
        return unexpected(ex, exchange);
    }

    private ProblemDetail unexpected(Throwable failure, ServerWebExchange exchange) {
        log.error("Unhandled failure on {} (request {})", exchange.getRequest().getPath(),
                RequestIds.of(exchange), failure);
        counter.count(failure, SOURCE_HTTP);

        return ProblemDetails.of(HttpStatus.INTERNAL_SERVER_ERROR, UNEXPECTED_FAILURE_CODE,
                "Unexpected error", "The request could not be completed.", exchange);
    }

}
