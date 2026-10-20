package com.calendar.chat.configuration;

import com.calendar.chat.exception.RequestIds;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class RequestIdExchangeFilterTest {

    private final AtomicReference<ClientRequest> sent = new AtomicReference<>();
    private final WebClient client = WebClient.builder()
            .filter(new RequestIdExchangeFilter())
            .exchangeFunction(request -> {
                sent.set(request);
                return Mono.just(ClientResponse.create(HttpStatus.OK).build());
            })
            .build();

    @Test
    void filter_shouldForwardTheRequestId_fromTheReactorContext() {
        client.get().uri("http://wely-users/x").retrieve().toBodilessEntity()
                .contextWrite(Context.of(RequestIds.REQUEST_ID_ATTRIBUTE, "abcd1234-ef56"))
                .block(Duration.ofSeconds(10));

        assertThat(sent.get().headers().getFirst(RequestIds.REQUEST_ID_HEADER)).isEqualTo("abcd1234-ef56");
    }

    @Test
    void filter_shouldSendNoHeader_outsideARequest() {
        client.get().uri("http://wely-users/x").retrieve().toBodilessEntity().block(Duration.ofSeconds(10));

        assertThat(sent.get().headers().containsHeader(RequestIds.REQUEST_ID_HEADER)).isFalse();
    }
}
