package com.calendar.chat.domain.services;

import com.calendar.chat.domain.ports.DeletedUserMarkers;
import com.calendar.chat.exception.BusinessErrorCode;
import com.calendar.chat.exception.BusinessException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

import java.time.Duration;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LiveSessionsTest {

    @Mock
    private DeletedUserMarkers markers;

    private LiveSessions sessions;

    @BeforeEach
    void setUp() {
        sessions = new LiveSessions(new AccountGuard(markers));
        lenient().when(markers.isDeleted(anyString())).thenReturn(Mono.just(false));
    }

    @Test
    void attach_shouldRelayTheStream_whenTheUserIsActive() {
        StepVerifier.create(sessions.attach("alice", Flux.just("a", "b"))).expectNext("a", "b").verifyComplete();
    }

    @Test
    @DisplayName("a deleted account's stream is refused before it opens: no presence, no subscription")
    void attach_shouldFailWithAccountDeleted_andNeverSubscribeTheStream_whenTheUserIsDeleted() {
        when(markers.isDeleted("alice")).thenReturn(Mono.just(true));
        TestPublisher<String> stream = TestPublisher.create();

        StepVerifier.create(sessions.attach("alice", stream.flux()))
                .expectErrorMatches(error -> error instanceof BusinessException business
                        && business.getErrorCode() == BusinessErrorCode.ACCOUNT_DELETED)
                .verify();

        stream.assertWasNotSubscribed();
        assertThat(sessions.sessionCount()).isZero();
    }

    @Test
    void attach_shouldForgetTheSession_whenTheStreamCompletesFailsOrIsCancelled() {
        StepVerifier.create(sessions.attach("alice", Flux.just("a"))).expectNext("a").verifyComplete();
        StepVerifier.create(sessions.attach("alice", Flux.error(new IllegalStateException("broker down"))))
                .verifyError(IllegalStateException.class);
        TestPublisher<String> open = TestPublisher.create();
        StepVerifier.create(sessions.attach("alice", open.flux()))
                .then(() -> assertThat(sessions.sessionCount()).isEqualTo(1))
                .thenCancel()
                .verify();

        assertThat(sessions.sessionCount()).isZero();
    }

    @Test
    @DisplayName("closing completes the stream, so the web app does not reconnect in a loop")
    void close_shouldCompleteEveryStreamOfTheUser_andLeaveOtherUsersOpen() {
        TestPublisher<String> aliceOnPhone = TestPublisher.create();
        TestPublisher<String> aliceOnLaptop = TestPublisher.create();
        TestPublisher<String> bob = TestPublisher.create();
        StepVerifier phone = StepVerifier.create(sessions.attach("alice", aliceOnPhone.flux()))
                .expectComplete().verifyLater();
        StepVerifier laptop = StepVerifier.create(sessions.attach("alice", aliceOnLaptop.flux()))
                .expectComplete().verifyLater();
        StepVerifier bobs = StepVerifier.create(sessions.attach("bob", bob.flux()))
                .expectNext("still here").expectComplete().verifyLater();

        StepVerifier.create(sessions.close("alice")).verifyComplete();

        phone.verify(Duration.ofSeconds(5));
        laptop.verify(Duration.ofSeconds(5));
        aliceOnPhone.assertCancelled();
        aliceOnLaptop.assertCancelled();
        assertThat(sessions.sessionCount()).isEqualTo(1);
        bob.emit("still here");
        bobs.verify(Duration.ofSeconds(5));
    }

    @Test
    void close_shouldDoNothing_whenTheUserHasNoStreamHere() {
        StepVerifier.create(sessions.close("nobody")).verifyComplete();

        assertThat(sessions.sessionCount()).isZero();
    }

    @Test
    void closeDeleted_shouldCloseOnlyTheStreamsOfDeletedUsers() {
        when(markers.findDeletedAmong(any())).thenAnswer(call -> Flux.fromIterable(call.<Collection<String>>getArgument(0))
                .filter("alice"::equals));
        TestPublisher<String> alice = TestPublisher.create();
        TestPublisher<String> bob = TestPublisher.create();
        StepVerifier alices = StepVerifier.create(sessions.attach("alice", alice.flux()))
                .expectComplete().verifyLater();
        StepVerifier.create(sessions.attach("bob", bob.flux())).expectComplete().verifyLater();

        StepVerifier.create(sessions.closeDeleted()).verifyComplete();

        alices.verify(Duration.ofSeconds(5));
        bob.assertNotCancelled();
        assertThat(sessions.sessionCount()).isEqualTo(1);
        verify(markers).findDeletedAmong(argThat(ids -> ids.size() == 2 && ids.containsAll(List.of("alice", "bob"))));
    }

    @Test
    void closeDeleted_shouldNotQuery_whenNoStreamIsOpen() {
        StepVerifier.create(sessions.closeDeleted()).verifyComplete();

        verify(markers, never()).findDeletedAmong(any());
    }
}
