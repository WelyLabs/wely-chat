package com.calendar.chat.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.http.server.reactive.ServerHttpResponseDecorator;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.oauth2.server.resource.web.server.BearerTokenServerAuthenticationEntryPoint;
import org.springframework.security.web.server.ServerAuthenticationEntryPoint;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * Answers a missing or invalid token with this service's problem, {@code CHT-REQ-001}.
 *
 * <p>The default entry point answers an empty 401. The {@code WWW-Authenticate: Bearer…} header
 * it sets is part of the OAuth 2.0 contract — it tells the client why the token was refused — so
 * the default still runs, for its headers only: its own empty answer is swallowed and the problem
 * written in its place.
 */
@Slf4j
@Component
public class ProblemAuthenticationEntryPoint implements ServerAuthenticationEntryPoint {

    private final ServerAuthenticationEntryPoint headers = new BearerTokenServerAuthenticationEntryPoint();
    private final ProblemResponseWriter writer;

    public ProblemAuthenticationEntryPoint(ProblemResponseWriter writer) {
        this.writer = writer;
    }

    @Override
    public Mono<Void> commence(ServerWebExchange exchange, AuthenticationException failure) {
        BusinessErrorCode error = BusinessErrorCode.AUTHENTICATION_REQUIRED;
        // An expired token is routine traffic, not an incident: DEBUG, without a stack trace.
        log.debug("Authentication failure {} on {} (request {})", error.getCode(), exchange.getRequest().getPath(),
                RequestIds.of(exchange));

        return headers.commence(headersOnly(exchange), failure)
                .then(Mono.defer(() -> writer.write(exchange, ProblemDetails.of(error, exchange))));
    }

    /** The same exchange, except that completing its response does not commit it. */
    static ServerWebExchange headersOnly(ServerWebExchange exchange) {
        ServerHttpResponse response = new ServerHttpResponseDecorator(exchange.getResponse()) {
            @Override
            public Mono<Void> setComplete() {
                return Mono.empty();
            }
        };
        return exchange.mutate().response(response).build();
    }
}
