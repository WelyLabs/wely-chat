package com.calendar.chat.configuration;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.List;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class CallsClientConfigTest {

    private final List<ClientRequest> sent = new ArrayList<>();

    private WebClient capturing(WebClient client) {
        return client.mutate()
                .exchangeFunction(request -> {
                    sent.add(request);
                    return Mono.just(ClientResponse.create(HttpStatus.NO_CONTENT).build());
                })
                .build();
    }

    private void call(WebClient client) {
        StepVerifier.create(client.post().uri("/apps/a/sessions/new").retrieve().toBodilessEntity())
                .expectNextCount(1)
                .expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void realtimeWebClient_shouldTargetRealtimeWithTheAppSecret() {
        call(capturing(new CallsClientConfig().realtimeWebClient("https://rtc.example/v1", "app-secret")));

        assertThat(sent.getFirst().url().toString()).isEqualTo("https://rtc.example/v1/apps/a/sessions/new");
        assertThat(sent.getFirst().headers().getFirst(HttpHeaders.AUTHORIZATION)).isEqualTo("Bearer app-secret");
    }

    @Test
    void turnWebClient_shouldAuthenticateWithTheTurnToken() {
        call(capturing(new CallsClientConfig().turnWebClient("https://rtc.example/v1", "turn-token")));

        assertThat(sent.getFirst().headers().getFirst(HttpHeaders.AUTHORIZATION)).isEqualTo("Bearer turn-token");
    }
}
