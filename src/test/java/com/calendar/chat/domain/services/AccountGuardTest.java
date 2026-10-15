package com.calendar.chat.domain.services;

import com.calendar.chat.domain.ports.DeletedUserMarkers;
import com.calendar.chat.exception.ChatErrorCode;
import com.calendar.chat.exception.ChatException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AccountGuardTest {

    @Mock
    private DeletedUserMarkers markers;

    private AccountGuard guard() {
        return new AccountGuard(markers);
    }

    @Test
    void requireActive_shouldComplete_whenNoMarkerExists() {
        when(markers.isDeleted("alice")).thenReturn(Mono.just(false));

        StepVerifier.create(guard().requireActive("alice")).verifyComplete();
    }

    @Test
    void requireActive_shouldFailWithAccountDeleted_whenAMarkerExists() {
        when(markers.isDeleted("alice")).thenReturn(Mono.just(true));

        StepVerifier.create(guard().requireActive("alice"))
                .expectErrorMatches(error -> error instanceof ChatException chat
                        && chat.getErrorCode() == ChatErrorCode.ACCOUNT_DELETED)
                .verify();
    }

    @Test
    void requireActive_shouldPropagateAStoreFailure() {
        when(markers.isDeleted("alice")).thenReturn(Mono.error(new ChatException(ChatErrorCode.STORE_UNAVAILABLE)));

        StepVerifier.create(guard().requireActive("alice"))
                .expectErrorMatches(error -> error instanceof ChatException chat
                        && chat.getErrorCode() == ChatErrorCode.STORE_UNAVAILABLE)
                .verify();
    }

    @Test
    void isDeleted_shouldReflectTheMarker() {
        when(markers.isDeleted("alice")).thenReturn(Mono.just(true));
        when(markers.isDeleted("bob")).thenReturn(Mono.just(false));

        StepVerifier.create(guard().isDeleted("alice")).expectNext(true).verifyComplete();
        StepVerifier.create(guard().isDeleted("bob")).expectNext(false).verifyComplete();
    }

    @Test
    void deletedAmong_shouldReturnOnlyTheMarkedIds_askingOnceForEachId() {
        when(markers.findDeletedAmong(List.of("alice", "bob"))).thenReturn(Flux.just("bob"));

        StepVerifier.create(guard().deletedAmong(List.of("alice", "bob", "alice")))
                .expectNext(Set.of("bob"))
                .verifyComplete();

        verify(markers).findDeletedAmong(List.of("alice", "bob"));
    }

    @Test
    void deletedAmong_shouldNotQuery_whenGivenNoIds() {
        StepVerifier.create(guard().deletedAmong(List.of())).expectNext(Set.of()).verifyComplete();
        StepVerifier.create(guard().deletedAmong(null)).expectNext(Set.of()).verifyComplete();

        verifyNoInteractions(markers);
    }
}
