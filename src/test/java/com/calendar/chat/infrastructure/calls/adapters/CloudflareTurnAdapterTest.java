package com.calendar.chat.infrastructure.calls.adapters;

import com.calendar.chat.domain.models.IceServer;
import com.calendar.chat.infrastructure.calls.mappers.RealtimeMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.mock.http.client.reactive.MockClientHttpRequest;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CloudflareTurnAdapterTest {

    private final List<MockClientHttpRequest> sent = new ArrayList<>();
    private ClientResponse answer;

    @BeforeEach
    void setUp() {
        answer = json(HttpStatus.CREATED, "{}");
    }

    private CloudflareTurnAdapter adapterFor(String keyId) {
        WebClient client = WebClient.builder()
                .baseUrl("https://rtc.example/v1")
                .exchangeFunction(request -> {
                    MockClientHttpRequest written = new MockClientHttpRequest(request.method(), request.url());
                    sent.add(written);
                    return request.writeTo(written, ExchangeStrategies.withDefaults()).then(Mono.fromSupplier(() -> answer));
                })
                .build();
        return new CloudflareTurnAdapter(client, keyId, Mappers.getMapper(RealtimeMapper.class));
    }

    private static ClientResponse json(HttpStatus status, String body) {
        return ClientResponse.create(status)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .body(body)
                .build();
    }

    @Test
    void iceServers_shouldAskForShortLivedCredentials_andAcceptOneUrlOrSeveral() {
        answer = json(HttpStatus.CREATED, """
                {"iceServers":[{"urls":"stun:stun.cloudflare.com:3478"},
                  {"urls":["turn:turn.cloudflare.com:3478?transport=udp","turns:turn.cloudflare.com:443?transport=tcp"],
                   "username":"u","credential":"c"}]}""");

        StepVerifier.create(adapterFor("key-1").iceServers())
                .expectNext(List.of(
                        new IceServer(List.of("stun:stun.cloudflare.com:3478"), null, null),
                        new IceServer(List.of("turn:turn.cloudflare.com:3478?transport=udp",
                                "turns:turn.cloudflare.com:443?transport=tcp"), "u", "c")))
                .verifyComplete();

        assertThat(sent.getFirst().getURI().toString())
                .isEqualTo("https://rtc.example/v1/turn/keys/key-1/credentials/generate-ice-servers");
        assertThat(sent.getFirst().getBodyAsString().block()).isEqualTo("{\"ttl\":14400}");
    }

    @Test
    void iceServers_shouldFallBackOnStun_withoutATurnKey() {
        StepVerifier.create(adapterFor("").iceServers()).expectNext(CloudflareTurnAdapter.STUN_ONLY).verifyComplete();

        assertThat(sent).isEmpty();
    }

    @Test
    void iceServers_shouldFallBackOnStun_whenCloudflareFails() {
        answer = json(HttpStatus.INTERNAL_SERVER_ERROR, "{}");

        StepVerifier.create(adapterFor("key-1").iceServers()).expectNext(CloudflareTurnAdapter.STUN_ONLY).verifyComplete();
    }

    @Test
    void iceServers_shouldFallBackOnStun_whenTheAnswerHasNoServer() {
        answer = json(HttpStatus.CREATED, "{\"iceServers\":[]}");

        StepVerifier.create(adapterFor("key-1").iceServers()).expectNext(CloudflareTurnAdapter.STUN_ONLY).verifyComplete();
    }
}
