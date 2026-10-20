package com.calendar.chat.exception;

import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.web.server.ServerWebExchange;

import java.net.URI;
import java.time.Instant;

/**
 * Builds every RFC 7807 problem this service answers, wherever it is written: the controller
 * advice, the security entry points and the last-resort {@code WebExceptionHandler} all go
 * through {@link #of}, so a client reads the same shape whichever layer refused the request.
 *
 * <p>Three properties are added beyond the standard fields: {@code code}, the stable
 * identifier a client branches on, {@code timestamp}, and {@code requestId} — see
 * {@link RequestIds#of}: the id the gateway issued, quoted in every log line of the request, so a
 * response leads straight to the logs that produced it.
 */
public final class ProblemDetails {

    public static final String CODE_PROPERTY = "code";
    public static final String TIMESTAMP_PROPERTY = "timestamp";
    public static final String REQUEST_ID_PROPERTY = "requestId";

    /** Namespace for the {@code type} URI. Not dereferenced; it identifies, it does not serve. */
    private static final String PROBLEM_TYPE_BASE = "https://welylabs.app/problems/";

    private ProblemDetails() {
    }

    public static ProblemDetail of(HttpStatusCode status, String code, String title, String detail,
                                   ServerWebExchange exchange) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail);
        problem.setType(typeOf(code));
        problem.setTitle(title);
        problem.setInstance(URI.create(exchange.getRequest().getPath().value()));
        problem.setProperty(CODE_PROPERTY, code);
        problem.setProperty(TIMESTAMP_PROPERTY, Instant.now().toString());
        problem.setProperty(REQUEST_ID_PROPERTY, RequestIds.of(exchange));

        return problem;
    }

    /** The {@code type} URI of a code: an identifier, not dereferenced. */
    public static URI typeOf(String code) {
        return URI.create(PROBLEM_TYPE_BASE + code.toLowerCase());
    }

    public static ProblemDetail of(BusinessErrorCode error, ServerWebExchange exchange) {
        return of(error.getHttpStatus(), error.getCode(), error.getTitle(), error.getDetail(), exchange);
    }
}
