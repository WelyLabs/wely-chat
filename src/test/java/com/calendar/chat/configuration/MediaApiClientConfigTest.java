package com.calendar.chat.configuration;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.List;

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
                .verifyComplete();

        assertThat(sent.getFirst().url().toString()).isEqualTo("http://wely-media:8085/internal/media");
        assertThat(sent.getFirst().headers().getFirst(MediaApiClientConfig.SECRET_HEADER)).isEqualTo("s3cret");
    }
}
