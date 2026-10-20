package com.calendar.chat.infrastructure.persistence.adapters;

import com.calendar.chat.domain.models.Call;
import com.calendar.chat.domain.models.CallMedia;
import com.calendar.chat.domain.models.CallParticipant;
import com.calendar.chat.domain.models.CallStatus;
import com.calendar.chat.exception.TechnicalErrorCode;
import com.calendar.chat.exception.TechnicalException;
import com.calendar.chat.infrastructure.persistence.mappers.CallPersistenceMapper;
import com.calendar.chat.infrastructure.persistence.models.entities.CallEntity;
import com.calendar.chat.infrastructure.persistence.repositories.CallMongoRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mapstruct.factory.Mappers;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DuplicateKeyException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MongoCallRepositoryAdapterTest {

    private static final Instant NOW = Instant.parse("2026-10-15T18:00:00Z");

    @Mock
    private CallMongoRepository repository;

    private final CallPersistenceMapper mapper = Mappers.getMapper(CallPersistenceMapper.class);
    private MongoCallRepositoryAdapter adapter;

    @BeforeEach
    void setUp() {
        adapter = new MongoCallRepositoryAdapter(repository, mapper);
    }

    private static Call call(CallStatus status, CallParticipant... participants) {
        return new Call("call-1", "conv-1", "alice", "Alice", CallMedia.AUDIO, status, NOW, null, List.of("alice", "bob"),
                List.of(participants));
    }

    private static CallParticipant bob() {
        return new CallParticipant("bob", "s-bob", NOW, NOW, List.of());
    }

    private void stored(Call call) {
        when(repository.findById("call-1")).thenReturn(Mono.just(mapper.toEntity(call)));
    }

    @Test
    void createOrGetLive_shouldSaveTheNewCall() {
        Call ringing = call(CallStatus.RINGING);
        when(repository.save(any())).thenReturn(Mono.just(mapper.toEntity(ringing)));

        StepVerifier.create(adapter.createOrGetLive(ringing)).expectNext(ringing).expectComplete().verify(Duration.ofSeconds(10));

        ArgumentCaptor<CallEntity> saved = ArgumentCaptor.forClass(CallEntity.class);
        verify(repository).save(saved.capture());
        assertThat(saved.getValue().getStartedByName()).isEqualTo("Alice");
    }

    @Test
    void createOrGetLive_shouldReturnTheCallStartedConcurrently_whenTheUniqueIndexRefusesTheNewOne() {
        Call other = call(CallStatus.RINGING);
        when(repository.save(any())).thenReturn(Mono.error(new DuplicateKeyException("live call")));
        when(repository.findByConversationIdAndLiveTrue("conv-1")).thenReturn(Mono.just(mapper.toEntity(other)));

        StepVerifier.create(adapter.createOrGetLive(call(CallStatus.RINGING))).expectNext(other).expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void findLive_shouldReadTheConversationsLiveCall() {
        when(repository.findByConversationIdAndLiveTrue("conv-1")).thenReturn(Mono.empty());

        StepVerifier.create(adapter.findLive("conv-1")).expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void findById_shouldBeScopedToTheMembers() {
        Call ringing = call(CallStatus.RINGING);
        when(repository.findMemberCall("call-1", "bob")).thenReturn(Mono.just(mapper.toEntity(ringing)));

        StepVerifier.create(adapter.findById("call-1", "bob")).expectNext(ringing).expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void addParticipant_shouldReplaceAPreviousEntry_thenTakeAPlace() {
        Call joined = call(CallStatus.RINGING, bob());
        when(repository.pullFromLive("call-1", "bob")).thenReturn(Mono.just(0L));
        when(repository.pushIfRoom(eq("call-1"), any(), eq(12))).thenReturn(Mono.just(1L));
        stored(joined);

        StepVerifier.create(adapter.addParticipant("call-1", bob(), 12)).expectNext(joined).expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void addParticipant_shouldBeEmpty_whenThereIsNoRoomLeft() {
        when(repository.pullFromLive("call-1", "bob")).thenReturn(Mono.just(0L));
        when(repository.pushIfRoom(eq("call-1"), any(), eq(2))).thenReturn(Mono.just(0L));

        StepVerifier.create(adapter.addParticipant("call-1", bob(), 2)).expectComplete().verify(Duration.ofSeconds(10));

        verify(repository, never()).findById("call-1");
    }

    @Test
    void markActive_shouldReadTheCallBack() {
        Call active = call(CallStatus.ACTIVE);
        when(repository.markActive("call-1")).thenReturn(Mono.just(1L));
        stored(active);

        StepVerifier.create(adapter.markActive("call-1")).expectNext(active).expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void addTracks_shouldReadTheCallBack_onceWritten() {
        Call call = call(CallStatus.ACTIVE, bob());
        when(repository.addTracks("call-1", "bob", List.of("audio"))).thenReturn(Mono.just(1L));
        stored(call);

        StepVerifier.create(adapter.addTracks("call-1", "bob", List.of("audio"))).expectNext(call).expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void touch_shouldBeEmpty_forSomeoneNotInTheCall() {
        when(repository.touch("call-1", "bob", NOW)).thenReturn(Mono.just(0L));

        StepVerifier.create(adapter.touch("call-1", "bob", NOW)).expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void removeParticipant_shouldReadTheCallBack_onceRemoved() {
        Call call = call(CallStatus.ACTIVE);
        when(repository.removeParticipant("call-1", "bob")).thenReturn(Mono.just(1L));
        stored(call);

        StepVerifier.create(adapter.removeParticipant("call-1", "bob")).expectNext(call).expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void end_shouldStoreTheStatusByName_andBeEmptyOnceAlreadyEnded() {
        when(repository.end("call-1", "MISSED", NOW)).thenReturn(Mono.just(0L));

        StepVerifier.create(adapter.end("call-1", CallStatus.MISSED, NOW)).expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void finders_shouldMapEveryCall() {
        Call call = call(CallStatus.RINGING);
        when(repository.findRingingStartedBefore(NOW)).thenReturn(Flux.just(mapper.toEntity(call)));
        when(repository.findLiveWithParticipantSeenBefore(NOW)).thenReturn(Flux.just(mapper.toEntity(call)));
        when(repository.findLiveStartedBefore(NOW)).thenReturn(Flux.<CallEntity>empty());

        StepVerifier.create(adapter.findRingingStartedBefore(NOW)).expectNext(call).expectComplete().verify(Duration.ofSeconds(10));
        StepVerifier.create(adapter.findLiveWithParticipantSeenBefore(NOW)).expectNext(call).expectComplete().verify(Duration.ofSeconds(10));
        StepVerifier.create(adapter.findLiveStartedBefore(NOW)).expectComplete().verify(Duration.ofSeconds(10));
    }

    private static boolean storeUnavailable(Throwable error) {
        return error instanceof TechnicalException technical && technical.getErrorCode() == TechnicalErrorCode.STORE_UNAVAILABLE;
    }

    @Test
    void findLiveInvolving_shouldMapTheCalls() {
        Call call = call(CallStatus.ACTIVE, bob());
        when(repository.findLiveInvolving("bob")).thenReturn(Flux.just(mapper.toEntity(call)));

        StepVerifier.create(adapter.findLiveInvolving("bob")).expectNext(call).expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void findLiveInvolving_shouldTranslateAStoreFailure() {
        when(repository.findLiveInvolving("bob"))
                .thenReturn(Flux.error(new DataAccessResourceFailureException("mongo down")));

        StepVerifier.create(adapter.findLiveInvolving("bob"))
                .expectErrorMatches(MongoCallRepositoryAdapterTest::storeUnavailable)
                .verify(Duration.ofSeconds(10));
    }

    @Test
    void removeFromEndedCalls_shouldPullTheSessions_thenEraseTheCallerName() {
        when(repository.pullFromEnded("bob")).thenReturn(Mono.just(4L));
        when(repository.clearStarterName("bob")).thenReturn(Mono.just(2L));

        StepVerifier.create(adapter.removeFromEndedCalls("bob")).expectComplete().verify(Duration.ofSeconds(10));

        InOrder order = inOrder(repository);
        order.verify(repository).pullFromEnded("bob");
        order.verify(repository).clearStarterName("bob");
    }

    @Test
    void removeFromEndedCalls_shouldTranslateAStoreFailure_whileErasingTheCallerName() {
        when(repository.pullFromEnded("bob")).thenReturn(Mono.just(0L));
        when(repository.clearStarterName("bob"))
                .thenReturn(Mono.error(new DataAccessResourceFailureException("mongo down")));

        StepVerifier.create(adapter.removeFromEndedCalls("bob"))
                .expectErrorMatches(MongoCallRepositoryAdapterTest::storeUnavailable)
                .verify(Duration.ofSeconds(10));
    }

    @Test
    void removeFromEndedCalls_shouldTranslateAStoreFailure() {
        when(repository.pullFromEnded("bob"))
                .thenReturn(Mono.error(new DataAccessResourceFailureException("mongo down")));

        StepVerifier.create(adapter.removeFromEndedCalls("bob"))
                .expectErrorMatches(MongoCallRepositoryAdapterTest::storeUnavailable)
                .verify(Duration.ofSeconds(10));
    }
}
