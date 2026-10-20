package com.calendar.chat.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.oauth2.server.resource.web.access.server.BearerTokenServerAccessDeniedHandler;
import org.springframework.security.web.server.authorization.ServerAccessDeniedHandler;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * Answers an authenticated caller refused by the security chain with this service's problem,
 * {@code CHT-REQ-002}.
 *
 * <p>Like {@link ProblemAuthenticationEntryPoint}, the default handler still runs for the
 * {@code WWW-Authenticate} header it sets; only its empty body is replaced.
 */
@Slf4j
@Component
public class ProblemAccessDeniedHandler implements ServerAccessDeniedHandler {

    private final ServerAccessDeniedHandler headers = new BearerTokenServerAccessDeniedHandler();
    private final ProblemResponseWriter writer;

    public ProblemAccessDeniedHandler(ProblemResponseWriter writer) {
        this.writer = writer;
    }

    @Override
    public Mono<Void> handle(ServerWebExchange exchange, AccessDeniedException denied) {
        BusinessErrorCode error = BusinessErrorCode.ACCESS_DENIED;
        // An authenticated caller reaching for what is not theirs is worth a look: WARN.
        log.warn("Access failure {} on {} (request {})", error.getCode(), exchange.getRequest().getPath(),
                RequestIds.of(exchange));

        return headers.handle(ProblemAuthenticationEntryPoint.headersOnly(exchange), denied)
                .then(Mono.defer(() -> writer.write(exchange, ProblemDetails.of(error, exchange))));
    }
}
