package com.calendar.chat.exception;

import org.junit.jupiter.api.Test;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;

import static org.assertj.core.api.Assertions.assertThat;

class RequestIdsTest {

    @Test
    void of_shouldReadTheAdoptedId_whenTheFilterStoredOne() {
        MockServerWebExchange exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/x"));
        exchange.getAttributes().put(RequestIds.REQUEST_ID_ATTRIBUTE, "abcd1234-ef56");

        assertThat(RequestIds.of(exchange)).isEqualTo("abcd1234-ef56");
    }

    @Test
    void of_shouldFallBackToWebFluxsId_whenNoneWasStored() {
        MockServerWebExchange exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/x"));

        assertThat(RequestIds.of(exchange)).isEqualTo(exchange.getRequest().getId());
    }
}
