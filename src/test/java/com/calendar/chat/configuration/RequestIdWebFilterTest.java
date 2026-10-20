package com.calendar.chat.configuration;

import com.calendar.chat.exception.RequestIds;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class RequestIdWebFilterTest {

    private static final String GATEWAY_ID = "3f2b8c1e-7d4a-4e9b-9a51-0c6e2f8d1b7a";

    private final RequestIdWebFilter filter = new RequestIdWebFilter();
    private final AtomicReference<String> seenInContext = new AtomicReference<>();
    private final WebFilterChain chain = exchange -> Mono.deferContextual(context -> {
        seenInContext.set(context.getOrDefault(RequestIds.REQUEST_ID_ATTRIBUTE, null));
        return Mono.empty();
    });

    private MockServerWebExchange run(MockServerHttpRequest.BaseBuilder<?> request) {
        MockServerWebExchange exchange = MockServerWebExchange.from(request);
        StepVerifier.create(filter.filter(exchange, chain)).expectComplete().verify(Duration.ofSeconds(10));
        return exchange;
    }

    @Test
    @DisplayName("the gateway's id is kept: as attribute, in the Reactor Context and on the response")
    void filter_shouldKeepAValidHeader_everywhere() {
        MockServerWebExchange exchange = run(MockServerHttpRequest.get("/x").header(RequestIds.REQUEST_ID_HEADER, GATEWAY_ID));

        assertThat(RequestIds.of(exchange)).isEqualTo(GATEWAY_ID);
        assertThat(seenInContext.get()).isEqualTo(GATEWAY_ID);
        assertThat(exchange.getResponse().getHeaders().getFirst(RequestIds.REQUEST_ID_HEADER)).isEqualTo(GATEWAY_ID);
    }

    @ParameterizedTest
    @ValueSource(strings = {"short", "has spaces in it", "line\nbreak-1234", "semi;colon;1234",
            "0123456789012345678901234567890123456789012345678901234567890123456789"})
    @DisplayName("an id that is too short, too long or carries other characters is replaced by WebFlux's own")
    void filter_shouldFallBack_whenTheHeaderIsInvalid(String sent) {
        MockServerWebExchange exchange = run(MockServerHttpRequest.get("/x").header(RequestIds.REQUEST_ID_HEADER, sent));

        assertThat(RequestIds.of(exchange)).isEqualTo(exchange.getRequest().getId()).isNotEqualTo(sent);
        assertThat(seenInContext.get()).isEqualTo(exchange.getRequest().getId());
        assertThat(exchange.getResponse().getHeaders().getFirst(RequestIds.REQUEST_ID_HEADER))
                .isEqualTo(exchange.getRequest().getId());
    }

    @Test
    void filter_shouldFallBack_whenTheHeaderIsMissing() {
        MockServerWebExchange exchange = run(MockServerHttpRequest.get("/x"));

        assertThat(RequestIds.of(exchange)).isEqualTo(exchange.getRequest().getId()).isNotBlank();
        assertThat(exchange.getResponse().getHeaders().getFirst(RequestIds.REQUEST_ID_HEADER))
                .isEqualTo(exchange.getRequest().getId());
    }
}
