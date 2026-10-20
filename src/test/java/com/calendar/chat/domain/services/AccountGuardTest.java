package com.calendar.chat.domain.services;

import com.calendar.chat.domain.ports.DeletedUserMarkers;
import com.calendar.chat.exception.BusinessErrorCode;
import com.calendar.chat.exception.BusinessException;
import com.calendar.chat.exception.TechnicalErrorCode;
import com.calendar.chat.exception.TechnicalException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Set;
import java.time.Duration;

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

        StepVerifier.create(guard().requireActive("alice")).expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void requireActive_shouldFailWithAccountDeleted_whenAMarkerExists() {
        when(markers.isDeleted("alice")).thenReturn(Mono.just(true));

        StepVerifier.create(guard().requireActive("alice"))
                .expectErrorMatches(error -> error instanceof BusinessException business
                        && business.getErrorCode() == BusinessErrorCode.ACCOUNT_DELETED)
                .verify(Duration.ofSeconds(10));
    }

    @Test
    void requireActive_shouldPropagateAStoreFailure() {
        when(markers.isDeleted("alice")).thenReturn(Mono.error(new TechnicalException(TechnicalErrorCode.STORE_UNAVAILABLE)));

        StepVerifier.create(guard().requireActive("alice"))
                .expectErrorMatches(error -> error instanceof TechnicalException technical
                        && technical.getErrorCode() == TechnicalErrorCode.STORE_UNAVAILABLE)
                .verify(Duration.ofSeconds(10));
    }

    @Test
    void isDeleted_shouldReflectTheMarker() {
        when(markers.isDeleted("alice")).thenReturn(Mono.just(true));
        when(markers.isDeleted("bob")).thenReturn(Mono.just(false));

        StepVerifier.create(guard().isDeleted("alice")).expectNext(true).expectComplete().verify(Duration.ofSeconds(10));
        StepVerifier.create(guard().isDeleted("bob")).expectNext(false).expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void deletedAmong_shouldReturnOnlyTheMarkedIds_askingOnceForEachId() {
        when(markers.findDeletedAmong(List.of("alice", "bob"))).thenReturn(Flux.just("bob"));

        StepVerifier.create(guard().deletedAmong(List.of("alice", "bob", "alice")))
                .expectNext(Set.of("bob"))
                .expectComplete().verify(Duration.ofSeconds(10));

        verify(markers).findDeletedAmong(List.of("alice", "bob"));
    }

    @Test
    void deletedAmong_shouldNotQuery_whenGivenNoIds() {
        StepVerifier.create(guard().deletedAmong(List.of())).expectNext(Set.of()).expectComplete().verify(Duration.ofSeconds(10));
        StepVerifier.create(guard().deletedAmong(null)).expectNext(Set.of()).expectComplete().verify(Duration.ofSeconds(10));

        verifyNoInteractions(markers);
    }
}
