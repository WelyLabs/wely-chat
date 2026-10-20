package com.calendar.chat.configuration;

import com.calendar.chat.exception.RequestIds;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;


/**
 * Adopts the gateway's {@value RequestIds#REQUEST_ID_HEADER}, so one id follows a request from the
 * gateway through this service's logs, its problem answers and the calls it makes to other Wely
 * services.
 *
 * <p>The header is taken only if it looks like an id — letters, digits and dashes, 8 to 64 of them;
 * anything else (missing, oversized, or carrying characters that could forge a log line) falls back
 * to WebFlux's own request id. The id is stored as the {@value RequestIds#REQUEST_ID_ATTRIBUTE}
 * exchange attribute, in the Reactor Context under the same key (where outgoing WebClients read it),
 * and echoed on the response. First of all filters, so even the security chain's refusals quote it.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class RequestIdWebFilter implements WebFilter {

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String sent = exchange.getRequest().getHeaders().getFirst(RequestIds.REQUEST_ID_HEADER);
        String id = RequestIds.isValid(sent) ? sent : exchange.getRequest().getId();

        exchange.getAttributes().put(RequestIds.REQUEST_ID_ATTRIBUTE, id);
        exchange.getResponse().getHeaders().set(RequestIds.REQUEST_ID_HEADER, id);
        return chain.filter(exchange).contextWrite(Context.of(RequestIds.REQUEST_ID_ATTRIBUTE, id));
    }
}
