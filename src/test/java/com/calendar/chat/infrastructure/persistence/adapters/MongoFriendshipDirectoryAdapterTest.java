package com.calendar.chat.infrastructure.persistence.adapters;

import com.calendar.chat.domain.models.Friendship;
import com.calendar.chat.exception.TechnicalErrorCode;
import com.calendar.chat.exception.TechnicalException;
import com.calendar.chat.infrastructure.persistence.mappers.FriendshipPersistenceMapper;
import com.calendar.chat.infrastructure.persistence.models.entities.FriendshipEntity;
import com.calendar.chat.infrastructure.persistence.repositories.FriendshipMongoRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mapstruct.factory.Mappers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DuplicateKeyException;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MongoFriendshipDirectoryAdapterTest {

    private static final String ALICE = "11111111-0000-4000-8000-000000000001";
    private static final String BOB = "22222222-0000-4000-8000-000000000002";
    private static final String PAIR = ALICE + ":" + BOB;

    @Mock
    private FriendshipMongoRepository repository;

    private MongoFriendshipDirectoryAdapter adapter;

    @BeforeEach
    void setUp() {
        adapter = new MongoFriendshipDirectoryAdapter(repository, Mappers.getMapper(FriendshipPersistenceMapper.class));
    }

    private static boolean storeUnavailable(Throwable error) {
        return error instanceof TechnicalException technical && technical.getErrorCode() == TechnicalErrorCode.STORE_UNAVAILABLE;
    }

    private static DataAccessResourceFailureException mongoDown() {
        return new DataAccessResourceFailureException("mongo down");
    }

    @Test
    void saveIfNewer_shouldInsertANewPairAsFriends() {
        when(repository.insert(any(FriendshipEntity.class))).thenAnswer(call -> Mono.just(call.getArgument(0)));

        StepVerifier.create(adapter.saveIfNewer(new Friendship(BOB, ALICE, 3))).expectNext(true).expectComplete().verify(Duration.ofSeconds(10));

        ArgumentCaptor<FriendshipEntity> inserted = ArgumentCaptor.forClass(FriendshipEntity.class);
        verify(repository).insert(inserted.capture());
        assertThat(inserted.getValue()).usingRecursiveComparison()
                .isEqualTo(new FriendshipEntity(PAIR, ALICE, BOB, true, 3));
        verify(repository, never()).befriendIfOlder(anyString(), anyLong());
    }

    @Test
    @DisplayName("a stored pair is updated only by a newer version")
    void saveIfNewer_shouldUpdateUnderTheVersionGuard_whenThePairIsStored() {
        when(repository.insert(any(FriendshipEntity.class))).thenReturn(Mono.error(new DuplicateKeyException("dup")));
        when(repository.befriendIfOlder(PAIR, 3)).thenReturn(Mono.just(1L));

        StepVerifier.create(adapter.saveIfNewer(new Friendship(ALICE, BOB, 3))).expectNext(true).expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void saveIfNewer_shouldAnswerFalse_whenTheStoredVersionIsNotOlder() {
        when(repository.insert(any(FriendshipEntity.class))).thenReturn(Mono.error(new DuplicateKeyException("dup")));
        when(repository.befriendIfOlder(PAIR, 3)).thenReturn(Mono.just(0L));

        StepVerifier.create(adapter.saveIfNewer(new Friendship(ALICE, BOB, 3))).expectNext(false).expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void saveIfNewer_shouldTranslateOtherErrors() {
        when(repository.insert(any(FriendshipEntity.class))).thenReturn(Mono.error(mongoDown()));

        StepVerifier.create(adapter.saveIfNewer(new Friendship(ALICE, BOB, 3)))
                .expectErrorMatches(MongoFriendshipDirectoryAdapterTest::storeUnavailable)
                .verify(Duration.ofSeconds(10));
    }

    @Test
    void saveIfNewer_shouldTranslateAFailedUpdate() {
        when(repository.insert(any(FriendshipEntity.class))).thenReturn(Mono.error(new DuplicateKeyException("dup")));
        when(repository.befriendIfOlder(PAIR, 3)).thenReturn(Mono.error(mongoDown()));

        StepVerifier.create(adapter.saveIfNewer(new Friendship(ALICE, BOB, 3)))
                .expectErrorMatches(MongoFriendshipDirectoryAdapterTest::storeUnavailable)
                .verify(Duration.ofSeconds(10));
    }

    @Test
    void end_shouldEndThePair_whicheverSideIsNamedFirst() {
        when(repository.endIfFriends(PAIR)).thenReturn(Mono.just(1L));

        StepVerifier.create(adapter.end(BOB, ALICE)).expectNext(true).expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void end_shouldAnswerFalse_whenThePairWasNotFriends() {
        when(repository.endIfFriends(PAIR)).thenReturn(Mono.just(0L));

        StepVerifier.create(adapter.end(ALICE, BOB)).expectNext(false).expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void end_shouldTranslateErrors() {
        when(repository.endIfFriends(PAIR)).thenReturn(Mono.error(mongoDown()));

        StepVerifier.create(adapter.end(ALICE, BOB))
                .expectErrorMatches(MongoFriendshipDirectoryAdapterTest::storeUnavailable)
                .verify(Duration.ofSeconds(10));
    }

    @Test
    void areFriends_shouldAskForThePair_whicheverSideIsNamedFirst() {
        when(repository.existsFriends(PAIR)).thenReturn(Mono.just(true));

        StepVerifier.create(adapter.areFriends(BOB, ALICE)).expectNext(true).expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void areFriends_shouldTranslateErrors() {
        when(repository.existsFriends(PAIR)).thenReturn(Mono.error(mongoDown()));

        StepVerifier.create(adapter.areFriends(ALICE, BOB))
                .expectErrorMatches(MongoFriendshipDirectoryAdapterTest::storeUnavailable)
                .verify(Duration.ofSeconds(10));
    }

    @Test
    void forget_shouldDeleteEveryPairOfTheAccount() {
        when(repository.deleteInvolving(ALICE)).thenReturn(Mono.just(2L));

        StepVerifier.create(adapter.forget(ALICE)).expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void forget_shouldTranslateErrors() {
        when(repository.deleteInvolving(ALICE)).thenReturn(Mono.error(mongoDown()));

        StepVerifier.create(adapter.forget(ALICE))
                .expectErrorMatches(MongoFriendshipDirectoryAdapterTest::storeUnavailable)
                .verify(Duration.ofSeconds(10));
    }
}
