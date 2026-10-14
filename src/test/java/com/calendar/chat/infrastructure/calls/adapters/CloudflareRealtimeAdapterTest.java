package com.calendar.chat.infrastructure.calls.adapters;

import com.calendar.chat.domain.models.LocalTrack;
import com.calendar.chat.domain.models.RelaySubscription;
import com.calendar.chat.domain.models.RemoteTrack;
import com.calendar.chat.domain.models.SessionDescription;
import com.calendar.chat.domain.models.VideoLayer;
import com.calendar.chat.exception.ChatErrorCode;
import com.calendar.chat.exception.ChatException;
import com.calendar.chat.infrastructure.calls.mappers.RealtimeMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
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

/** Requests go to a stub exchange: what is checked is the JSON Cloudflare receives and how its answer is read. */
class CloudflareRealtimeAdapterTest {

    private static final String BASE = "https://rtc.example/v1";

    private final List<MockClientHttpRequest> sent = new ArrayList<>();
    private ClientResponse answer;
    private CloudflareRealtimeAdapter adapter;

    @BeforeEach
    void setUp() {
        answer = json(HttpStatus.OK, "{}");
        adapter = adapterFor("app-1");
    }

    private CloudflareRealtimeAdapter adapterFor(String appId) {
        WebClient client = WebClient.builder()
                .baseUrl(BASE)
                .exchangeFunction(request -> {
                    MockClientHttpRequest written = new MockClientHttpRequest(request.method(), request.url());
                    sent.add(written);
                    return request.writeTo(written, ExchangeStrategies.withDefaults()).then(Mono.fromSupplier(() -> answer));
                })
                .build();
        return new CloudflareRealtimeAdapter(client, appId, Mappers.getMapper(RealtimeMapper.class));
    }

    private static ClientResponse json(HttpStatus status, String body) {
        return ClientResponse.create(status)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .body(body)
                .build();
    }

    private String sentBody() {
        return sent.getFirst().getBodyAsString().block();
    }

    private static boolean unavailable(Throwable error) {
        return error instanceof ChatException chat && chat.getErrorCode() == ChatErrorCode.CALLS_UNAVAILABLE;
    }

    @Test
    void newSession_shouldCreateASessionInTheApp() {
        answer = json(HttpStatus.CREATED, "{\"sessionId\":\"s-1\"}");

        StepVerifier.create(adapter.newSession()).expectNext("s-1").verifyComplete();

        assertThat(sent.getFirst().getMethod()).isEqualTo(HttpMethod.POST);
        assertThat(sent.getFirst().getURI().toString()).isEqualTo(BASE + "/apps/app-1/sessions/new");
    }

    @Test
    void newSession_shouldFail_withoutASessionInTheAnswer() {
        StepVerifier.create(adapter.newSession()).expectErrorMatches(CloudflareRealtimeAdapterTest::unavailable).verify();
    }

    @Test
    void publish_shouldSendTheOfferAndLocalTracks_andReturnTheAnswer() {
        answer = json(HttpStatus.OK, "{\"sessionDescription\":{\"type\":\"answer\",\"sdp\":\"v=0 a\"},\"tracks\":[]}");

        StepVerifier.create(adapter.publish("s-1", new SessionDescription("offer", "v=0 o"),
                        List.of(new LocalTrack("0", "audio"))))
                .expectNext(SessionDescription.answer("v=0 a"))
                .verifyComplete();

        assertThat(sent.getFirst().getURI().toString()).isEqualTo(BASE + "/apps/app-1/sessions/s-1/tracks/new");
        assertThat(sentBody())
                .contains("\"sessionDescription\":{\"type\":\"offer\",\"sdp\":\"v=0 o\"}")
                .contains("{\"location\":\"local\",\"mid\":\"0\",\"trackName\":\"audio\"}");
    }

    @Test
    void subscribe_shouldAskForRemoteTracks_withoutAnOffer_andKeepOnlyTheTracksThatSucceeded() {
        answer = json(HttpStatus.OK, """
                {"requiresImmediateRenegotiation":true,
                 "sessionDescription":{"type":"offer","sdp":"v=0 r"},
                 "tracks":[{"mid":"3","trackName":"camera","sessionId":"s-alice"},
                           {"trackName":"audio","sessionId":"s-gone","errorCode":"not_found"}]}""");

        StepVerifier.create(adapter.subscribe("s-1",
                        List.of(new RemoteTrack("s-alice", "camera", VideoLayer.FULL, null))))
                .expectNext(new RelaySubscription(new SessionDescription("offer", "v=0 r"), true,
                        List.of(new RemoteTrack("s-alice", "camera", null, "3"))))
                .verifyComplete();

        assertThat(sentBody())
                .doesNotContain("sessionDescription")
                .contains("\"location\":\"remote\"")
                .contains("\"sessionId\":\"s-alice\"")
                .contains("\"preferredRid\":\"f\"");
    }

    @Test
    void subscribe_shouldNeedNoRenegotiation_whenTheRelaySaysSo() {
        answer = json(HttpStatus.OK, "{\"tracks\":[]}");

        StepVerifier.create(adapter.subscribe("s-1", List.of()))
                .expectNext(new RelaySubscription(null, false, List.of()))
                .verifyComplete();
    }

    @Test
    void renegotiate_shouldPutTheAnswer() {
        StepVerifier.create(adapter.renegotiate("s-1", SessionDescription.answer("v=0 b"))).verifyComplete();

        assertThat(sent.getFirst().getMethod()).isEqualTo(HttpMethod.PUT);
        assertThat(sent.getFirst().getURI().toString()).isEqualTo(BASE + "/apps/app-1/sessions/s-1/renegotiate");
        assertThat(sentBody()).contains("\"type\":\"answer\"");
    }

    @Test
    void updateLayers_shouldPutTheTracksWithTheirMid() {
        StepVerifier.create(adapter.updateLayers("s-1",
                List.of(new RemoteTrack("s-alice", "camera", VideoLayer.QUARTER, "3")))).verifyComplete();

        assertThat(sent.getFirst().getURI().toString()).isEqualTo(BASE + "/apps/app-1/sessions/s-1/tracks/update");
        assertThat(sentBody()).contains("\"mid\":\"3\"").contains("\"preferredRid\":\"q\"");
    }

    @Test
    void closeTracks_shouldForceTheClose() {
        StepVerifier.create(adapter.closeTracks("s-1", List.of("3"))).verifyComplete();

        assertThat(sent.getFirst().getURI().toString()).isEqualTo(BASE + "/apps/app-1/sessions/s-1/tracks/close");
        assertThat(sentBody()).contains("\"force\":true").contains("\"mid\":\"3\"");
    }

    @Test
    void anyCall_shouldFail_whenRealtimeAnswersAnErrorStatus() {
        answer = json(HttpStatus.UNAUTHORIZED, "{\"errorCode\":\"unauthorized\"}");

        StepVerifier.create(adapter.newSession()).expectErrorMatches(CloudflareRealtimeAdapterTest::unavailable).verify();
    }

    @Test
    void anyCall_shouldFail_whenRealtimeAnswers200WithAnErrorCode() {
        answer = json(HttpStatus.OK, "{\"errorCode\":\"invalid_sdp\",\"errorDescription\":\"bad\"}");

        StepVerifier.create(adapter.renegotiate("s-1", SessionDescription.answer("x")))
                .expectErrorMatches(CloudflareRealtimeAdapterTest::unavailable)
                .verify();
    }

    @Test
    void anyCall_shouldFail_whenRealtimeIsUnreachable() {
        WebClient broken = WebClient.builder().baseUrl(BASE)
                .exchangeFunction(request -> Mono.error(new IllegalStateException("connection refused")))
                .build();
        adapter = new CloudflareRealtimeAdapter(broken, "app-1", Mappers.getMapper(RealtimeMapper.class));

        StepVerifier.create(adapter.newSession()).expectErrorMatches(CloudflareRealtimeAdapterTest::unavailable).verify();
    }

    @Test
    void anyCall_shouldFailWithoutCallingOut_whenNoAppIsConfigured() {
        adapter = adapterFor("");

        StepVerifier.create(adapter.newSession()).expectErrorMatches(CloudflareRealtimeAdapterTest::unavailable).verify();

        assertThat(sent).isEmpty();
    }
}
