package com.calendar.chat.domain.services;

import com.calendar.chat.domain.models.ConversationDetail;
import com.calendar.chat.domain.models.Presence;
import com.calendar.chat.domain.ports.ChatRepository;
import com.calendar.chat.domain.ports.DeletedUserMarkers;
import com.calendar.chat.domain.ports.PresenceRepository;
import com.calendar.chat.exception.ChatErrorCode;
import com.calendar.chat.exception.ChatException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PresenceServiceTest {

    private static final Instant NOW = Instant.parse("2026-10-11T18:00:00Z");

    @Mock
    private PresenceRepository presenceRepository;
    @Mock
    private ChatRepository chatRepository;
    @Mock
    private DeletedUserMarkers markers;

    private PresenceService service;

    @BeforeEach
    void setUp() {
        service = new PresenceService(presenceRepository, chatRepository, new AccountGuard(markers), "pod-1",
                Clock.fixed(NOW, ZoneOffset.UTC));
        // Nobody is deleted unless a test says so.
        lenient().when(markers.isDeleted(anyString())).thenReturn(Mono.just(false));
        lenient().when(markers.findDeletedAmong(any())).thenReturn(Flux.empty());
    }

    private void deleted(String... userIds) {
        lenient().when(markers.isDeleted(anyString()))
                .thenAnswer(call -> Mono.just(List.of(userIds).contains(call.<String>getArgument(0))));
        lenient().when(markers.findDeletedAmong(any())).thenAnswer(call -> Flux.fromIterable(call.<Collection<String>>getArgument(0))
                .filter(List.of(userIds)::contains));
    }

    private void connectionsSucceed() {
        when(presenceRepository.connect(anyString(), anyString(), anyString(), any())).thenReturn(Mono.empty());
        // Lenient: a test that leaves its streams open never disconnects.
        lenient().when(presenceRepository.disconnect(anyString(), anyString(), any())).thenReturn(Mono.empty());
    }

    @Test
    void track_shouldRecordTheConnectionWhileTheStreamIsOpen_andForgetItWhenItCompletes() {
        connectionsSucceed();

        StepVerifier.create(service.track("alice", Flux.just("a", "b")))
                .expectNext("a", "b")
                .verifyComplete();

        ArgumentCaptor<String> connectionId = ArgumentCaptor.forClass(String.class);
        verify(presenceRepository).connect(connectionId.capture(), eq("alice"), eq("pod-1"), eq(NOW));
        verify(presenceRepository).disconnect(connectionId.getValue(), "alice", NOW);
        assertThat(service.localConnectionCount()).isZero();
    }

    @Test
    void track_shouldForgetTheConnection_whenTheClientGoesAway() {
        // Closing the app cancels the stream: that must take the user offline too.
        connectionsSucceed();
        TestPublisher<String> stream = TestPublisher.create();

        StepVerifier.create(service.track("alice", stream.flux()))
                .then(() -> assertThat(service.localConnectionCount()).isEqualTo(1))
                .thenCancel()
                .verify();

        verify(presenceRepository).disconnect(anyString(), eq("alice"), eq(NOW));
        assertThat(service.localConnectionCount()).isZero();
    }

    @Test
    void track_shouldForgetTheConnection_whenTheStreamFails() {
        connectionsSucceed();

        StepVerifier.create(service.track("alice", Flux.error(new IllegalStateException("broker down"))))
                .verifyError(IllegalStateException.class);

        verify(presenceRepository).disconnect(anyString(), eq("alice"), eq(NOW));
    }

    @Test
    void track_shouldStillOpenTheStream_whenPresenceCannotBeRecorded() {
        when(presenceRepository.connect(anyString(), anyString(), anyString(), any()))
                .thenReturn(Mono.error(new IllegalStateException("mongo down")));
        when(presenceRepository.disconnect(anyString(), anyString(), any()))
                .thenReturn(Mono.error(new IllegalStateException("mongo down")));

        StepVerifier.create(service.track("alice", Flux.just("hello")))
                .expectNext("hello")
                .verifyComplete();
    }

    @Test
    @DisplayName("a stream refused before it is subscribed leaves nothing for the heartbeat to refresh")
    void track_shouldRegisterNoConnection_untilSubscribed() {
        service.track("alice", Flux.just("a"));

        assertThat(service.localConnectionCount()).isZero();
        StepVerifier.create(service.heartbeat()).verifyComplete();
        verify(presenceRepository, never()).connect(anyString(), anyString(), anyString(), any());
        verify(presenceRepository, never()).refresh(any(), any());
    }

    @Test
    void heartbeat_shouldDoNothing_withoutOpenConnections() {
        StepVerifier.create(service.heartbeat()).verifyComplete();

        verify(presenceRepository, never()).refresh(any(), any());
    }

    @Test
    void heartbeat_shouldRefreshEveryConnectionOpenOnThisInstance() {
        connectionsSucceed();
        when(presenceRepository.refresh(any(), any())).thenReturn(Mono.empty());
        TestPublisher<String> alice = TestPublisher.create();
        TestPublisher<String> bob = TestPublisher.create();
        service.track("alice", alice.flux()).subscribe();
        service.track("bob", bob.flux()).subscribe();

        StepVerifier.create(service.heartbeat()).verifyComplete();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, String>> refreshed = ArgumentCaptor.forClass(Map.class);
        verify(presenceRepository).refresh(refreshed.capture(), eq(NOW));
        assertThat(refreshed.getValue().values()).containsExactlyInAnyOrder("alice", "bob");
    }

    @Test
    @DisplayName("an open stream of a deleted account does not bring its last-seen time back")
    void heartbeat_shouldNotRefreshTheConnectionsOfDeletedUsers() {
        connectionsSucceed();
        when(presenceRepository.refresh(any(), any())).thenReturn(Mono.empty());
        service.track("alice", TestPublisher.<String>create().flux()).subscribe();
        service.track("bob", TestPublisher.<String>create().flux()).subscribe();
        deleted("bob");

        StepVerifier.create(service.heartbeat()).verifyComplete();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, String>> refreshed = ArgumentCaptor.forClass(Map.class);
        verify(presenceRepository).refresh(refreshed.capture(), eq(NOW));
        assertThat(refreshed.getValue().values()).containsExactly("alice");
    }

    @Test
    void heartbeat_shouldWriteNothing_whenEveryConnectedUserIsDeleted() {
        connectionsSucceed();
        service.track("bob", TestPublisher.<String>create().flux()).subscribe();
        deleted("bob");

        StepVerifier.create(service.heartbeat()).verifyComplete();

        verify(presenceRepository, never()).refresh(any(), any());
    }

    @Test
    @DisplayName("a stream closing after the account was deleted forgets the presence instead of recording it")
    void track_shouldForgetThePresence_insteadOfRecordingLastSeen_whenTheUserIsDeletedAtClose() {
        when(presenceRepository.connect(anyString(), anyString(), anyString(), any())).thenReturn(Mono.empty());
        when(presenceRepository.forget("bob")).thenReturn(Mono.empty());
        TestPublisher<String> stream = TestPublisher.create();

        StepVerifier.create(service.track("bob", stream.flux()))
                .then(() -> deleted("bob"))
                .then(stream::complete)
                .verifyComplete();

        verify(presenceRepository).forget("bob");
        verify(presenceRepository, never()).disconnect(anyString(), anyString(), any());
        assertThat(service.localConnectionCount()).isZero();
    }

    @Test
    void getPresence_shouldAnswerOfflineWithoutLastSeen_whenTheUserIsDeleted() {
        when(chatRepository.findByParticipantIds(List.of("alice", "bob")))
                .thenReturn(Mono.just(mock(ConversationDetail.class)));
        deleted("bob");

        StepVerifier.create(service.getPresence("alice", "bob"))
                .expectNext(new Presence("bob", false, null))
                .verifyComplete();
        verify(presenceRepository, never()).find(any(), any());
    }

    @Test
    void getPresence_shouldAnswer_forSomeoneTheCallerSharesAConversationWith() {
        Presence presence = new Presence("bob", true, NOW);
        when(chatRepository.findByParticipantIds(List.of("alice", "bob")))
                .thenReturn(Mono.just(mock(ConversationDetail.class)));
        when(presenceRepository.find("bob", NOW.minus(PresenceService.ONLINE_WINDOW))).thenReturn(Mono.just(presence));

        StepVerifier.create(service.getPresence("alice", "bob"))
                .expectNext(presence)
                .verifyComplete();
    }

    @Test
    void getPresence_shouldRefuse_forAStranger() {
        when(chatRepository.findByParticipantIds(List.of("alice", "carol"))).thenReturn(Mono.empty());

        StepVerifier.create(service.getPresence("alice", "carol"))
                .expectErrorMatches(error -> error instanceof ChatException e
                        && e.getErrorCode() == ChatErrorCode.PRESENCE_NOT_FOUND)
                .verify();
        verify(presenceRepository, never()).find(any(), any());
    }

    @Test
    void onlineWindow_shouldCoverSeveralHeartbeats() {
        assertThat(PresenceService.ONLINE_WINDOW).isGreaterThan(PresenceService.HEARTBEAT.multipliedBy(2));
    }
}
