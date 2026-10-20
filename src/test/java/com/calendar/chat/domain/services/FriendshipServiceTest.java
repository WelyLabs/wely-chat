package com.calendar.chat.domain.services;

import com.calendar.chat.domain.models.Friendship;
import com.calendar.chat.domain.ports.DeletedUserMarkers;
import com.calendar.chat.domain.ports.FriendshipDirectory;
import com.calendar.chat.exception.BusinessErrorCode;
import com.calendar.chat.exception.BusinessException;
import com.calendar.chat.exception.TechnicalErrorCode;
import com.calendar.chat.exception.TechnicalException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FriendshipServiceTest {

    private static final String ALICE = "11111111-0000-4000-8000-000000000001";
    private static final String BOB = "22222222-0000-4000-8000-000000000002";

    @Mock
    private FriendshipDirectory directory;

    @Mock
    private DeletedUserMarkers markers;

    private FriendshipService service;

    @BeforeEach
    void setUp() {
        service = new FriendshipService(directory, new AccountGuard(markers));
    }

    private static boolean friendshipRequired(Throwable error) {
        return error instanceof BusinessException business && business.getErrorCode() == BusinessErrorCode.FRIENDSHIP_REQUIRED;
    }

    @Test
    void applyState_shouldStoreThePair_whenNeitherIsDeleted() {
        Friendship friendship = new Friendship(ALICE, BOB, 4);
        when(markers.findDeletedAmong(any())).thenReturn(Flux.empty());
        when(directory.saveIfNewer(friendship)).thenReturn(Mono.just(true));

        StepVerifier.create(service.applyState(friendship)).expectNext(true).verifyComplete();
    }

    @Test
    @DisplayName("a late record cannot make a deleted account someone's friend again")
    void applyState_shouldStoreNothing_whenOneOfThemIsDeleted() {
        when(markers.findDeletedAmong(any())).thenReturn(Flux.just(BOB));

        StepVerifier.create(service.applyState(new Friendship(ALICE, BOB, 4))).expectNext(false).verifyComplete();

        verify(directory, never()).saveIfNewer(any());
    }

    @Test
    void applyTombstone_shouldEndThePair() {
        when(directory.end(ALICE, BOB)).thenReturn(Mono.just(true));

        StepVerifier.create(service.applyTombstone(ALICE, BOB)).expectNext(true).verifyComplete();
    }

    @Test
    void requireFriends_shouldComplete_whenTheyAreFriends() {
        when(directory.areFriends(ALICE, BOB)).thenReturn(Mono.just(true));

        StepVerifier.create(service.requireFriends(ALICE, BOB)).verifyComplete();
    }

    @Test
    void requireFriends_shouldFailWithFriendshipRequired_whenTheyAreNot() {
        when(directory.areFriends(ALICE, BOB)).thenReturn(Mono.just(false));

        StepVerifier.create(service.requireFriends(ALICE, BOB))
                .expectErrorMatches(FriendshipServiceTest::friendshipRequired)
                .verify();
    }

    @Test
    void requireFriends_shouldFailWithoutAsking_forOneself() {
        StepVerifier.create(service.requireFriends(ALICE, ALICE))
                .expectErrorMatches(FriendshipServiceTest::friendshipRequired)
                .verify();

        verifyNoInteractions(directory);
    }

    @Test
    @DisplayName("a private conversation that lost its other participant is nobody's friend")
    void requireFriends_shouldFailWithoutAsking_whenTheOtherIsMissing() {
        StepVerifier.create(service.requireFriends(ALICE, null))
                .expectErrorMatches(FriendshipServiceTest::friendshipRequired)
                .verify();

        verify(directory, never()).areFriends(anyString(), anyString());
    }

    @Test
    void requireFriends_shouldPassOnAStoreFailure() {
        when(directory.areFriends(ALICE, BOB)).thenReturn(Mono.error(new TechnicalException(TechnicalErrorCode.STORE_UNAVAILABLE)));

        StepVerifier.create(service.requireFriends(ALICE, BOB))
                .expectErrorMatches(error -> error instanceof TechnicalException technical
                        && technical.getErrorCode() == TechnicalErrorCode.STORE_UNAVAILABLE)
                .verify();
    }

    @Test
    void forget_shouldRemoveTheAccountsPairs() {
        when(directory.forget(ALICE)).thenReturn(Mono.empty());

        StepVerifier.create(service.forget(ALICE)).verifyComplete();

        verify(directory).forget(ALICE);
    }
}
