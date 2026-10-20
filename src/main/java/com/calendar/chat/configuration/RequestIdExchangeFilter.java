package com.calendar.chat.configuration;

import com.calendar.chat.exception.RequestIds;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.ExchangeFunction;
import reactor.core.publisher.Mono;

/**
 * Forwards the current request id to another Wely service in {@value RequestIds#REQUEST_ID_HEADER},
 * read from the Reactor Context where {@link RequestIdWebFilter} put it. A call made outside a
 * request — the reconciliation loop — has none and goes without.
 *
 * <p>For Wely services only: an external API (Cloudflare) has no use for our ids, and is not told them.
 */
public class RequestIdExchangeFilter implements ExchangeFilterFunction {

    @Override
    public Mono<ClientResponse> filter(ClientRequest request, ExchangeFunction next) {
        return Mono.deferContextual(context -> next.exchange(context.<String>getOrEmpty(RequestIds.REQUEST_ID_ATTRIBUTE)
                .map(id -> ClientRequest.from(request).headers(headers -> headers.set(RequestIds.REQUEST_ID_HEADER, id))
                        .build())
                .orElse(request)));
    }
}
