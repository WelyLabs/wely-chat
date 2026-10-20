package com.calendar.chat.domain.services;

import com.calendar.chat.domain.ports.ChatRepository;
import com.calendar.chat.domain.ports.DeletedUserMarkers;
import com.calendar.chat.domain.ports.PresenceRepository;
import com.calendar.chat.exception.TechnicalErrorCode;
import com.calendar.chat.exception.TechnicalException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.Duration;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AccountErasureServiceTest {

    private static final Instant NOW = Instant.parse("2026-10-15T14:05:00Z");
    private static final Instant DELETED_AT = Instant.parse("2026-10-15T14:03:27.123Z");
    private static final String USER = "89a380b8-0000-4000-8000-000000000001";

    @Mock
    private DeletedUserMarkers markers;
    @Mock
    private ChatRepository chatRepository;
    @Mock
    private PresenceRepository presenceRepository;
    @Mock
    private CallService callService;
    @Mock
    private LiveSessions liveSessions;
    @Mock
    private FriendshipService friendships;

    private AccountErasureService service;

    @BeforeEach
    void setUp() {
        service = new AccountErasureService(markers, chatRepository, presenceRepository, callService, liveSessions,
                friendships, Clock.fixed(NOW, ZoneOffset.UTC));
    }

    /** Lenient: a failing step leaves the later ones unused. */
    private void everyStepSucceeds() {
        lenient().when(markers.markDeleted(anyString(), any())).thenReturn(Mono.empty());
        lenient().when(liveSessions.close(anyString())).thenReturn(Mono.empty());
        lenient().when(chatRepository.anonymizeSender(anyString())).thenReturn(Mono.empty());
        lenient().when(chatRepository.markDirectConversationsReadOnly(anyString())).thenReturn(Mono.empty());
        lenient().when(chatRepository.forgetClosedGroupsOrganizedBy(anyString())).thenReturn(Mono.empty());
        lenient().when(callService.removeUser(anyString())).thenReturn(Mono.empty());
        lenient().when(presenceRepository.forget(anyString())).thenReturn(Mono.empty());
        lenient().when(friendships.forget(anyString())).thenReturn(Mono.empty());
        lenient().when(markers.markErased(anyString(), any())).thenReturn(Mono.empty());
    }

    @Test
    @DisplayName("the marker comes first, so every guard refuses the account before its data is touched")
    void erase_shouldRunEveryStepInOrder_thenMarkErased() {
        everyStepSucceeds();

        StepVerifier.create(service.erase(USER, DELETED_AT)).expectComplete().verify(Duration.ofSeconds(10));

        InOrder order = inOrder(markers, liveSessions, chatRepository, callService, presenceRepository, friendships);
        order.verify(markers).markDeleted(USER, DELETED_AT);
        order.verify(liveSessions).close(USER);
        order.verify(chatRepository).anonymizeSender(USER);
        order.verify(chatRepository).markDirectConversationsReadOnly(USER);
        order.verify(chatRepository).forgetClosedGroupsOrganizedBy(USER);
        order.verify(callService).removeUser(USER);
        order.verify(presenceRepository).forget(USER);
        order.verify(friendships).forget(USER);
        order.verify(markers).markErased(USER, NOW);
    }

    @Test
    @DisplayName("a record sent before deletedAt existed is erased with the processing time")
    void erase_shouldUseTheProcessingTimeAsDeletedAt_whenTheRecordHasNone() {
        everyStepSucceeds();

        StepVerifier.create(service.erase(USER, null)).expectComplete().verify(Duration.ofSeconds(10));

        verify(markers).markDeleted(USER, NOW);
    }

    @Test
    void erase_shouldNotMarkErased_whenAStepFails() {
        everyStepSucceeds();
        when(chatRepository.anonymizeSender(USER)).thenReturn(Mono.error(new TechnicalException(TechnicalErrorCode.STORE_UNAVAILABLE)));

        StepVerifier.create(service.erase(USER, DELETED_AT))
                .expectErrorMatches(error -> error instanceof TechnicalException technical
                        && technical.getErrorCode() == TechnicalErrorCode.STORE_UNAVAILABLE)
                .verify(Duration.ofSeconds(10));

        verify(chatRepository, never()).markDirectConversationsReadOnly(any());
        verify(callService, never()).removeUser(any());
        verify(presenceRepository, never()).forget(any());
        verify(friendships, never()).forget(any());
        verify(markers, never()).markErased(any(), any());
    }

    @Test
    void erase_shouldFailWithoutTouchingData_whenTheMarkerCannotBeWritten() {
        when(markers.markDeleted(USER, DELETED_AT))
                .thenReturn(Mono.error(new TechnicalException(TechnicalErrorCode.STORE_UNAVAILABLE)));

        StepVerifier.create(service.erase(USER, DELETED_AT)).expectError(TechnicalException.class).verify(Duration.ofSeconds(10));

        verifyNoInteractions(liveSessions, chatRepository, callService, presenceRepository, friendships);
        verify(markers, never()).markErased(any(), any());
    }

    @Test
    @DisplayName("a redelivered record runs every step again: a crash after the marker is repaired")
    void erase_shouldRunEveryStepAgain_whenTheMarkerAlreadyExists() {
        everyStepSucceeds();

        StepVerifier.create(service.erase(USER, DELETED_AT).then(service.erase(USER, DELETED_AT))).expectComplete().verify(Duration.ofSeconds(10));

        verify(markers, times(2)).markDeleted(USER, DELETED_AT);
        verify(chatRepository, times(2)).anonymizeSender(USER);
        verify(callService, times(2)).removeUser(USER);
        verify(presenceRepository, times(2)).forget(USER);
        verify(markers, times(2)).markErased(USER, NOW);
    }
}
