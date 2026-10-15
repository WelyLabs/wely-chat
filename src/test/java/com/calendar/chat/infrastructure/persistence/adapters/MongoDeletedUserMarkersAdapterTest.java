package com.calendar.chat.infrastructure.persistence.adapters;

import com.calendar.chat.domain.models.DeletedUserMarker;
import com.calendar.chat.exception.ChatErrorCode;
import com.calendar.chat.exception.ChatException;
import com.calendar.chat.infrastructure.persistence.mappers.DeletedUserMarkerMapper;
import com.calendar.chat.infrastructure.persistence.models.entities.DeletedUserMarkerEntity;
import com.calendar.chat.infrastructure.persistence.repositories.DeletedUserMarkerMongoRepository;
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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MongoDeletedUserMarkersAdapterTest {

    private static final Instant DELETED_AT = Instant.parse("2026-10-15T14:03:27.123Z");
    private static final Instant ERASED_AT = Instant.parse("2026-10-15T14:03:29Z");

    @Mock
    private DeletedUserMarkerMongoRepository repository;

    private MongoDeletedUserMarkersAdapter adapter;

    @BeforeEach
    void setUp() {
        adapter = new MongoDeletedUserMarkersAdapter(repository, Mappers.getMapper(DeletedUserMarkerMapper.class));
    }

    private static boolean storeUnavailable(Throwable error) {
        return error instanceof ChatException chat && chat.getErrorCode() == ChatErrorCode.STORE_UNAVAILABLE;
    }

    private static DataAccessResourceFailureException mongoDown() {
        return new DataAccessResourceFailureException("mongo down");
    }

    @Test
    void markDeleted_shouldInsertTheMarker() {
        when(repository.insert(any(DeletedUserMarkerEntity.class))).thenAnswer(call -> Mono.just(call.getArgument(0)));

        StepVerifier.create(adapter.markDeleted("alice", DELETED_AT)).verifyComplete();

        ArgumentCaptor<DeletedUserMarkerEntity> inserted = ArgumentCaptor.forClass(DeletedUserMarkerEntity.class);
        verify(repository).insert(inserted.capture());
        assertThat(inserted.getValue()).usingRecursiveComparison()
                .isEqualTo(new DeletedUserMarkerEntity("alice", DELETED_AT, null));
    }

    @Test
    @DisplayName("a replayed record finds the marker there: not an error, and the first deletedAt stays")
    void markDeleted_shouldSucceed_whenTheMarkerAlreadyExists() {
        when(repository.insert(any(DeletedUserMarkerEntity.class))).thenReturn(Mono.error(new DuplicateKeyException("dup")));

        StepVerifier.create(adapter.markDeleted("alice", DELETED_AT)).verifyComplete();
    }

    @Test
    void markDeleted_shouldTranslateOtherErrors() {
        when(repository.insert(any(DeletedUserMarkerEntity.class))).thenReturn(Mono.error(mongoDown()));

        StepVerifier.create(adapter.markDeleted("alice", DELETED_AT))
                .expectErrorMatches(MongoDeletedUserMarkersAdapterTest::storeUnavailable)
                .verify();
    }

    @Test
    void markErased_shouldSetErasedAt() {
        when(repository.markErased("alice", ERASED_AT)).thenReturn(Mono.just(1L));

        StepVerifier.create(adapter.markErased("alice", ERASED_AT)).verifyComplete();

        verify(repository).markErased("alice", ERASED_AT);
    }

    @Test
    void markErased_shouldTranslateAStoreFailure() {
        when(repository.markErased("alice", ERASED_AT)).thenReturn(Mono.error(mongoDown()));

        StepVerifier.create(adapter.markErased("alice", ERASED_AT))
                .expectErrorMatches(MongoDeletedUserMarkersAdapterTest::storeUnavailable)
                .verify();
    }

    @Test
    void find_shouldMapTheMarker() {
        when(repository.findById("alice")).thenReturn(Mono.just(new DeletedUserMarkerEntity("alice", DELETED_AT, ERASED_AT)));

        StepVerifier.create(adapter.find("alice"))
                .expectNext(new DeletedUserMarker("alice", DELETED_AT, ERASED_AT))
                .verifyComplete();
    }

    @Test
    void find_shouldBeEmpty_whenThereIsNoMarker() {
        when(repository.findById("bob")).thenReturn(Mono.empty());

        StepVerifier.create(adapter.find("bob")).verifyComplete();
    }

    @Test
    void find_shouldTranslateAStoreFailure() {
        when(repository.findById("alice")).thenReturn(Mono.error(mongoDown()));

        StepVerifier.create(adapter.find("alice"))
                .expectErrorMatches(MongoDeletedUserMarkersAdapterTest::storeUnavailable)
                .verify();
    }

    @Test
    void isDeleted_shouldDelegateToExistsById() {
        when(repository.existsById("alice")).thenReturn(Mono.just(true));

        StepVerifier.create(adapter.isDeleted("alice")).expectNext(true).verifyComplete();
    }

    @Test
    @DisplayName("a guard that cannot read its markers answers 502, not 500")
    void isDeleted_shouldTranslateAStoreFailure() {
        when(repository.existsById("alice")).thenReturn(Mono.error(mongoDown()));

        StepVerifier.create(adapter.isDeleted("alice"))
                .expectErrorMatches(MongoDeletedUserMarkersAdapterTest::storeUnavailable)
                .verify();
    }

    @Test
    void findDeletedAmong_shouldReturnTheIdsFound() {
        when(repository.findIdsIn(List.of("alice", "bob"))).thenReturn(Flux.just(new DeletedUserMarkerEntity("bob", null, null)));

        StepVerifier.create(adapter.findDeletedAmong(List.of("alice", "bob"))).expectNext("bob").verifyComplete();
    }

    @Test
    void findDeletedAmong_shouldTranslateAStoreFailure() {
        when(repository.findIdsIn(List.of("alice"))).thenReturn(Flux.error(mongoDown()));

        StepVerifier.create(adapter.findDeletedAmong(List.of("alice")))
                .expectErrorMatches(MongoDeletedUserMarkersAdapterTest::storeUnavailable)
                .verify();
    }
}
