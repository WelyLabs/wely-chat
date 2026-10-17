package com.calendar.chat.application.rest;

import com.calendar.chat.application.dtos.CallDTO;
import com.calendar.chat.application.dtos.LayersRequest;
import com.calendar.chat.application.dtos.PublishRequest;
import com.calendar.chat.application.dtos.SessionDescriptionDTO;
import com.calendar.chat.application.dtos.StartCallRequest;
import com.calendar.chat.application.dtos.SubscribeRequest;
import com.calendar.chat.application.dtos.SubscriptionResponse;
import com.calendar.chat.application.dtos.UnsubscribeRequest;
import com.calendar.chat.application.mappers.CallRestMapper;
import com.calendar.chat.domain.models.Call;
import com.calendar.chat.domain.models.CallJoin;
import com.calendar.chat.domain.models.CallMedia;
import com.calendar.chat.domain.models.CallParticipant;
import com.calendar.chat.domain.models.CallStatus;
import com.calendar.chat.domain.models.CallSubscription;
import com.calendar.chat.domain.models.IceServer;
import com.calendar.chat.domain.models.LayerChoice;
import com.calendar.chat.domain.models.LocalTrack;
import com.calendar.chat.domain.models.SessionDescription;
import com.calendar.chat.domain.models.TrackRequest;
import com.calendar.chat.domain.models.VideoLayer;
import com.calendar.chat.domain.services.CallService;
import com.calendar.chat.exception.ChatErrorCode;
import com.calendar.chat.exception.ChatException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mapstruct.factory.Mappers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CallControllerTest {

    private static final Instant NOW = Instant.parse("2026-10-15T18:00:00Z");

    @Mock
    private CallService callService;
    @Mock
    private Jwt jwt;

    private CallController controller;

    @BeforeEach
    void setUp() {
        controller = new CallController(callService, Mappers.getMapper(CallRestMapper.class));
        lenient().when(jwt.getClaimAsString("businessId")).thenReturn("bob");
        lenient().when(jwt.getClaimAsString("preferred_username")).thenReturn("Bob");
    }

    private static Call call() {
        return new Call("call-1", "conv-1", "alice", "Alice", CallMedia.VIDEO, CallStatus.ACTIVE, NOW, null,
                List.of("alice", "bob"), List.of(new CallParticipant("alice", "s-alice", NOW, NOW, List.of("audio"))));
    }

    @Test
    @DisplayName("the caller comes from the token, never from the body")
    void start_shouldStartForTheTokensUser() {
        when(callService.start("bob", "Bob", "conv-1", CallMedia.AUDIO)).thenReturn(Mono.just(call()));

        StepVerifier.create(controller.start(jwt, new StartCallRequest("conv-1", "AUDIO")))
                .assertNext(dto -> {
                    assertThat(dto.id()).isEqualTo("call-1");
                    assertThat(dto.startedByName()).isEqualTo("Alice");
                })
                .verifyComplete();
    }

    @Test
    void start_shouldPassANullName_whenTheTokenHasNone() {
        when(jwt.getClaimAsString("preferred_username")).thenReturn(null);
        when(callService.start("bob", null, "conv-1", CallMedia.AUDIO)).thenReturn(Mono.just(call()));

        StepVerifier.create(controller.start(jwt, new StartCallRequest("conv-1", "AUDIO")))
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    void start_shouldRefuseAnUnknownMedia_with400() {
        assertThatThrownBy(() -> controller.start(jwt, new StartCallRequest("conv-1", "HOLOGRAM")))
                .isInstanceOfSatisfying(ResponseStatusException.class,
                        error -> assertThat(error.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST));
        verifyNoInteractions(callService);
    }

    @Test
    void start_shouldRefuseAMissingConversation_with400() {
        assertThatThrownBy(() -> controller.start(jwt, new StartCallRequest(null, "AUDIO")))
                .isInstanceOf(ResponseStatusException.class);
    }

    @Test
    void start_shouldFailWithAccountDeleted_whenTheCallerIsDeleted() {
        when(callService.start("bob", "Bob", "conv-1", CallMedia.AUDIO))
                .thenReturn(Mono.error(new ChatException(ChatErrorCode.ACCOUNT_DELETED)));

        StepVerifier.create(controller.start(jwt, new StartCallRequest("conv-1", "AUDIO")))
                .expectErrorMatches(error -> error instanceof ChatException chat
                        && chat.getErrorCode() == ChatErrorCode.ACCOUNT_DELETED)
                .verify();
    }

    @Test
    void start_shouldFailWithReadOnly_whenTheConversationIsReadOnly() {
        when(callService.start("bob", "Bob", "conv-1", CallMedia.AUDIO))
                .thenReturn(Mono.error(new ChatException(ChatErrorCode.CONVERSATION_READ_ONLY)));

        StepVerifier.create(controller.start(jwt, new StartCallRequest("conv-1", "AUDIO")))
                .expectErrorMatches(error -> error instanceof ChatException chat
                        && chat.getErrorCode() == ChatErrorCode.CONVERSATION_READ_ONLY)
                .verify();
    }

    @Test
    void live_shouldAnswerTheLiveCall_or204() {
        when(callService.liveCall("bob", "conv-1")).thenReturn(Mono.just(call()));
        when(callService.liveCall("bob", "conv-2")).thenReturn(Mono.empty());

        StepVerifier.create(controller.live(jwt, "conv-1"))
                .assertNext(response -> assertThat(response.getBody().id()).isEqualTo("call-1"))
                .verifyComplete();
        StepVerifier.create(controller.live(jwt, "conv-2"))
                .assertNext(response -> assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT))
                .verifyComplete();
    }

    @Test
    @DisplayName("joining hands back the ICE servers, never a relay session id")
    void join_shouldAnswerTheIceServers_withoutTheSessionId() {
        IceServer turn = new IceServer(List.of("turn:t:3478"), "u", "c");
        when(callService.join("bob", "call-1")).thenReturn(Mono.just(new CallJoin(call(), "s-bob", List.of(turn))));

        StepVerifier.create(controller.join(jwt, "call-1"))
                .assertNext(response -> {
                    assertThat(response.iceServers()).singleElement()
                            .satisfies(server -> assertThat(server.credential()).isEqualTo("c"));
                    assertThat(response.toString()).doesNotContain("s-bob").doesNotContain("s-alice");
                })
                .verifyComplete();
    }

    @Test
    void publish_shouldRelayTheOfferAndTracks() {
        SessionDescription offer = new SessionDescription("offer", "v=0 o");
        when(callService.publish("bob", "call-1", offer, List.of(new LocalTrack("0", "audio"))))
                .thenReturn(Mono.just(SessionDescription.answer("v=0 a")));

        StepVerifier.create(controller.publish(jwt, "call-1", new PublishRequest(new SessionDescriptionDTO("offer", "v=0 o"),
                        List.of(new PublishRequest.Track("0", "audio")))))
                .expectNext(new SessionDescriptionDTO("answer", "v=0 a"))
                .verifyComplete();
    }

    @Test
    void publish_shouldRefuseAMissingOffer_with400() {
        assertThatThrownBy(() -> controller.publish(jwt, "call-1", new PublishRequest(null, List.of())))
                .isInstanceOf(ResponseStatusException.class);
    }

    @Test
    void subscribe_shouldNameTheLayer_andAnswerTracksByPerson() {
        when(callService.subscribe("bob", "call-1", List.of(new TrackRequest("alice", "camera", VideoLayer.HALF))))
                .thenReturn(Mono.just(new CallSubscription(new SessionDescription("offer", "v=0 r"), true,
                        List.of(new CallSubscription.ReceivedTrack("3", "alice", "camera")))));

        StepVerifier.create(controller.subscribe(jwt, "call-1",
                        new SubscribeRequest(List.of(new SubscribeRequest.Track("alice", "camera", "HALF")))))
                .expectNext(new SubscriptionResponse(new SessionDescriptionDTO("offer", "v=0 r"), true,
                        List.of(new SubscriptionResponse.Track("3", "alice", "camera"))))
                .verifyComplete();
    }

    @Test
    void subscribe_shouldRefuseAnUnknownLayer_with400() {
        assertThatThrownBy(() -> controller.subscribe(jwt, "call-1",
                new SubscribeRequest(List.of(new SubscribeRequest.Track("alice", "camera", "ULTRA")))))
                .isInstanceOf(ResponseStatusException.class);
    }

    @Test
    void renegotiate_shouldRelayTheAnswer() {
        when(callService.renegotiate("bob", "call-1", SessionDescription.answer("v=0"))).thenReturn(Mono.empty());

        StepVerifier.create(controller.renegotiate(jwt, "call-1", new SessionDescriptionDTO("answer", "v=0")))
                .verifyComplete();
    }

    @Test
    void layers_shouldRelayTheChoices() {
        when(callService.chooseLayers("bob", "call-1", List.of(new LayerChoice("3", "alice", "camera", VideoLayer.QUARTER))))
                .thenReturn(Mono.empty());

        StepVerifier.create(controller.layers(jwt, "call-1",
                new LayersRequest(List.of(new LayersRequest.Choice("3", "alice", "camera", "QUARTER"))))).verifyComplete();
    }

    @Test
    void unsubscribe_shouldTreatAMissingListAsEmpty() {
        when(callService.unsubscribe("bob", "call-1", List.of())).thenReturn(Mono.empty());

        StepVerifier.create(controller.unsubscribe(jwt, "call-1", new UnsubscribeRequest(null))).verifyComplete();
    }

    @Test
    void heartbeatLeaveAndDecline_shouldActForTheTokensUser() {
        when(callService.heartbeat("bob", "call-1")).thenReturn(Mono.empty());
        when(callService.leave("bob", "call-1")).thenReturn(Mono.empty());
        when(callService.decline("bob", "call-1")).thenReturn(Mono.empty());

        StepVerifier.create(controller.heartbeat(jwt, "call-1")).verifyComplete();
        StepVerifier.create(controller.leave(jwt, "call-1")).verifyComplete();
        StepVerifier.create(controller.decline(jwt, "call-1")).verifyComplete();

        verify(callService).heartbeat("bob", "call-1");
        verify(callService).leave("bob", "call-1");
        verify(callService).decline("bob", "call-1");
    }
}
