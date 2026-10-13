package com.calendar.chat.infrastructure.media.adapters;

import com.calendar.chat.infrastructure.media.signing.GarageRequestSigner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import com.calendar.chat.exception.ChatErrorCode;
import com.calendar.chat.exception.ChatException;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Requests go to a stub exchange: what is checked is what the adapter sends, not Garage. */
class GaragePhotoStorageAdapterTest {

    private final List<ClientRequest> sent = new ArrayList<>();
    private HttpStatus answer;
    private GaragePhotoStorageAdapter storage;

    @BeforeEach
    void setUp() {
        answer = HttpStatus.OK;
        WebClient client = WebClient.builder()
                .exchangeFunction(request -> {
                    sent.add(request);
                    return Mono.just(ClientResponse.create(answer).build());
                })
                .build();
        GarageRequestSigner signer = new GarageRequestSigner("GK000000000000000000000000",
                "0000000000000000000000000000000000000000000000000000000000000000", "garage", Clock.systemUTC());
        storage = new GaragePhotoStorageAdapter(client, signer, URI.create("http://dev-wely-garage-service:3900"),
                URI.create("https://dev-media.welylabs.app"), "wely-chat-media");
    }

    @Test
    @DisplayName("a signed URL names the public host, the bucket path and its expiry, and asks for a private cache")
    void signedUrl_shouldTargetThePublicHostWithAnExpiry() {
        URI url = URI.create(storage.signedUrl("chat/c1/m1.jpg", Duration.ofMinutes(10)));

        assertThat(url.getScheme()).isEqualTo("https");
        assertThat(url.getHost()).isEqualTo("dev-media.welylabs.app");
        assertThat(url.getPath()).isEqualTo("/wely-chat-media/chat/c1/m1.jpg");
        assertThat(url.getQuery())
                .contains("X-Amz-Expires=600")
                .contains("X-Amz-Signature=")
                .contains("response-cache-control=private, max-age=3600");
    }

    @Test
    void put_shouldWriteTheObjectInternally_withItsTypeAndAPrivateCacheControl() {
        StepVerifier.create(storage.put("chat/c1/m1.jpg", new byte[] {1}, "image/jpeg")).verifyComplete();

        ClientRequest request = sent.getFirst();
        assertThat(request.method()).isEqualTo(HttpMethod.PUT);
        assertThat(request.url().getAuthority()).isEqualTo("dev-wely-garage-service:3900");
        assertThat(request.url().getPath()).isEqualTo("/wely-chat-media/chat/c1/m1.jpg");
        assertThat(request.url().getQuery()).contains("X-Amz-Expires=60").contains("X-Amz-Signature=");
        assertThat(request.headers().getFirst(HttpHeaders.CONTENT_TYPE)).isEqualTo("image/jpeg");
        assertThat(request.headers().getFirst(HttpHeaders.CACHE_CONTROL)).isEqualTo(GaragePhotoStorageAdapter.CACHE_CONTROL);
    }

    @Test
    void put_shouldSignOnlyWhenSubscribed() {
        // Signed lazily, so a retry or a late subscription never sends an already-ageing URL.
        storage.put("chat/c1/m1.jpg", new byte[] {1}, "image/jpeg");

        assertThat(sent).isEmpty();
    }

    @Test
    void put_shouldFail_whenGarageRefuses() {
        answer = HttpStatus.FORBIDDEN;

        StepVerifier.create(storage.put("chat/c1/m1.jpg", new byte[] {1}, "image/jpeg"))
                // Whatever Garage answered, the caller sees the storage as unavailable.
                .expectErrorMatches(error -> error instanceof ChatException chat
                        && chat.getErrorCode() == ChatErrorCode.MEDIA_UNAVAILABLE)
                .verify();
    }

    @Test
    void delete_shouldRemoveTheObjectInternally() {
        answer = HttpStatus.NO_CONTENT;

        StepVerifier.create(storage.delete("chat/c1/m1.jpg")).verifyComplete();

        ClientRequest request = sent.getFirst();
        assertThat(request.method()).isEqualTo(HttpMethod.DELETE);
        assertThat(request.url().getAuthority()).isEqualTo("dev-wely-garage-service:3900");
        assertThat(request.url().getPath()).isEqualTo("/wely-chat-media/chat/c1/m1.jpg");
    }

    @Test
    void delete_shouldFail_whenGarageIsDown() {
        answer = HttpStatus.SERVICE_UNAVAILABLE;

        StepVerifier.create(storage.delete("chat/c1/m1.jpg"))
                // Whatever Garage answered, the caller sees the storage as unavailable.
                .expectErrorMatches(error -> error instanceof ChatException chat
                        && chat.getErrorCode() == ChatErrorCode.MEDIA_UNAVAILABLE)
                .verify();
    }
}
