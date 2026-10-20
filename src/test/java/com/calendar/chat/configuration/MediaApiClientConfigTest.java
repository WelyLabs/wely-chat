package com.calendar.chat.configuration;

import com.calendar.chat.exception.RequestIds;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

import java.util.ArrayList;
import java.util.List;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class MediaApiClientConfigTest {

    @Test
    void mediaWebClient_shouldTargetTheMediaServiceWithTheInternalSecret() {
        List<ClientRequest> sent = new ArrayList<>();
        WebClient client = new MediaApiClientConfig().mediaWebClient("http://wely-media:8085", "s3cret")
                .mutate()
                .exchangeFunction(request -> {
                    sent.add(request);
                    return Mono.just(ClientResponse.create(HttpStatus.NO_CONTENT).build());
                })
                .build();

        StepVerifier.create(client.delete().uri("/internal/media").retrieve().toBodilessEntity())
                .expectNextCount(1)
                .expectComplete().verify(Duration.ofSeconds(10));

        assertThat(sent.getFirst().url().toString()).isEqualTo("http://wely-media:8085/internal/media");
        assertThat(sent.getFirst().headers().getFirst(MediaApiClientConfig.SECRET_HEADER)).isEqualTo("s3cret");
    }

    @Test
    @DisplayName("wely-media is a Wely service: it receives the request id")
    void mediaWebClient_shouldForwardTheRequestId() {
        List<ClientRequest> sent = new ArrayList<>();
        WebClient client = new MediaApiClientConfig().mediaWebClient("http://wely-media:8085", "s3cret")
                .mutate()
                .exchangeFunction(request -> {
                    sent.add(request);
                    return Mono.just(ClientResponse.create(HttpStatus.NO_CONTENT).build());
                })
                .build();

        StepVerifier.create(client.delete().uri("/internal/media").retrieve().toBodilessEntity()
                        .contextWrite(Context.of(RequestIds.REQUEST_ID_ATTRIBUTE, "abcd1234-ef56")))
                .expectNextCount(1)
                .expectComplete().verify(Duration.ofSeconds(10));

        assertThat(sent.getFirst().headers().getFirst(RequestIds.REQUEST_ID_HEADER)).isEqualTo("abcd1234-ef56");
    }
}
