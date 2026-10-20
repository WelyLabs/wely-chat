package com.calendar.chat.exception;

import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebExceptionHandler;
import reactor.core.publisher.Mono;

/**
 * Answers this service's problem for failures no controller advice sees: those raised in a
 * {@code WebFilter} or before a handler is chosen.
 *
 * <p>Without it they reach Boot's {@code DefaultErrorWebExceptionHandler}, ordered -1, which
 * answers its own JSON — no {@code code}, no {@code requestId}. Ordered just before it, and
 * mapped by {@link GlobalErrorHandler#handle}, so the status, code, log level and technical
 * counter are the same as for a failure raised in a controller.
 */
@Component
@Order(-2)
public class ProblemWebExceptionHandler implements WebExceptionHandler {

    private final GlobalErrorHandler errors;
    private final ProblemResponseWriter writer;

    public ProblemWebExceptionHandler(GlobalErrorHandler errors, ProblemResponseWriter writer) {
        this.errors = errors;
        this.writer = writer;
    }

    @Override
    public Mono<Void> handle(ServerWebExchange exchange, Throwable failure) {
        // Part of the response is already on the wire: nothing can be rewritten, let it fail.
        if (exchange.getResponse().isCommitted()) {
            return Mono.error(failure);
        }
        return writer.write(exchange, errors.handle(failure, exchange));
    }
}
