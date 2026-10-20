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
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class UsersApiClientConfigTest {

    private final List<ClientRequest> sent = new ArrayList<>();

    private WebClient stubbed(WebClient client) {
        return client.mutate()
                .exchangeFunction(request -> {
                    sent.add(request);
                    return Mono.just(ClientResponse.create(HttpStatus.OK).build());
                })
                .build();
    }

    @Test
    void usersWebClient_shouldTargetWelyUsersWithTheReaderSecret() {
        WebClient client = stubbed(new UsersApiClientConfig().usersWebClient("http://wely-users-service:8082", "s3cret"));

        StepVerifier.create(client.get().uri("/user-service/internal/deleted-users").retrieve().toBodilessEntity())
                .expectNextCount(1)
                .expectComplete().verify(Duration.ofSeconds(10));

        assertThat(sent.getFirst().url().toString())
                .isEqualTo("http://wely-users-service:8082/user-service/internal/deleted-users");
        assertThat(sent.getFirst().headers().getFirst(UsersApiClientConfig.SECRET_HEADER)).isEqualTo("s3cret");
    }

    @Test
    void usersWebClient_shouldSetNoBaseUrlNorSecret_whenNoneIsConfigured() {
        WebClient client = stubbed(new UsersApiClientConfig().usersWebClient("", ""));

        StepVerifier.create(client.get().uri("http://elsewhere/x").retrieve().toBodilessEntity())
                .expectNextCount(1)
                .expectComplete().verify(Duration.ofSeconds(10));

        assertThat(sent.getFirst().url().toString()).isEqualTo("http://elsewhere/x");
        assertThat(sent.getFirst().headers().getFirst(UsersApiClientConfig.SECRET_HEADER)).isNull();
    }
}
