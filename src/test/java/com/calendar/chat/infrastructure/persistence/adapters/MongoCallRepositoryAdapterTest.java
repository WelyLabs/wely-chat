package com.calendar.chat.infrastructure.persistence.adapters;

import com.calendar.chat.domain.models.Call;
import com.calendar.chat.domain.models.CallMedia;
import com.calendar.chat.domain.models.CallParticipant;
import com.calendar.chat.domain.models.CallStatus;
import com.calendar.chat.exception.ChatErrorCode;
import com.calendar.chat.exception.ChatException;
import com.calendar.chat.infrastructure.persistence.mappers.CallPersistenceMapper;
import com.calendar.chat.infrastructure.persistence.models.entities.CallEntity;
import com.calendar.chat.infrastructure.persistence.repositories.CallMongoRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mapstruct.factory.Mappers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DuplicateKeyException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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
        return new Call("call-1", "conv-1", "alice", CallMedia.AUDIO, status, NOW, null, List.of("alice", "bob"),
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

        StepVerifier.create(adapter.createOrGetLive(ringing)).expectNext(ringing).verifyComplete();
    }

    @Test
    void createOrGetLive_shouldReturnTheCallStartedConcurrently_whenTheUniqueIndexRefusesTheNewOne() {
        Call other = call(CallStatus.RINGING);
        when(repository.save(any())).thenReturn(Mono.error(new DuplicateKeyException("live call")));
        when(repository.findByConversationIdAndLiveTrue("conv-1")).thenReturn(Mono.just(mapper.toEntity(other)));

        StepVerifier.create(adapter.createOrGetLive(call(CallStatus.RINGING))).expectNext(other).verifyComplete();
    }

    @Test
    void findLive_shouldReadTheConversationsLiveCall() {
        when(repository.findByConversationIdAndLiveTrue("conv-1")).thenReturn(Mono.empty());

        StepVerifier.create(adapter.findLive("conv-1")).verifyComplete();
    }

    @Test
    void findById_shouldBeScopedToTheMembers() {
        Call ringing = call(CallStatus.RINGING);
        when(repository.findMemberCall("call-1", "bob")).thenReturn(Mono.just(mapper.toEntity(ringing)));

        StepVerifier.create(adapter.findById("call-1", "bob")).expectNext(ringing).verifyComplete();
    }

    @Test
    void addParticipant_shouldReplaceAPreviousEntry_thenTakeAPlace() {
        Call joined = call(CallStatus.RINGING, bob());
        when(repository.pullFromLive("call-1", "bob")).thenReturn(Mono.just(0L));
        when(repository.pushIfRoom(eq("call-1"), any(), eq(12))).thenReturn(Mono.just(1L));
        stored(joined);

        StepVerifier.create(adapter.addParticipant("call-1", bob(), 12)).expectNext(joined).verifyComplete();
    }

    @Test
    void addParticipant_shouldBeEmpty_whenThereIsNoRoomLeft() {
        when(repository.pullFromLive("call-1", "bob")).thenReturn(Mono.just(0L));
        when(repository.pushIfRoom(eq("call-1"), any(), eq(2))).thenReturn(Mono.just(0L));

        StepVerifier.create(adapter.addParticipant("call-1", bob(), 2)).verifyComplete();

        verify(repository, never()).findById("call-1");
    }

    @Test
    void markActive_shouldReadTheCallBack() {
        Call active = call(CallStatus.ACTIVE);
        when(repository.markActive("call-1")).thenReturn(Mono.just(1L));
        stored(active);

        StepVerifier.create(adapter.markActive("call-1")).expectNext(active).verifyComplete();
    }

    @Test
    void addTracks_shouldReadTheCallBack_onceWritten() {
        Call call = call(CallStatus.ACTIVE, bob());
        when(repository.addTracks("call-1", "bob", List.of("audio"))).thenReturn(Mono.just(1L));
        stored(call);

        StepVerifier.create(adapter.addTracks("call-1", "bob", List.of("audio"))).expectNext(call).verifyComplete();
    }

    @Test
    void touch_shouldBeEmpty_forSomeoneNotInTheCall() {
        when(repository.touch("call-1", "bob", NOW)).thenReturn(Mono.just(0L));

        StepVerifier.create(adapter.touch("call-1", "bob", NOW)).verifyComplete();
    }

    @Test
    void removeParticipant_shouldReadTheCallBack_onceRemoved() {
        Call call = call(CallStatus.ACTIVE);
        when(repository.removeParticipant("call-1", "bob")).thenReturn(Mono.just(1L));
        stored(call);

        StepVerifier.create(adapter.removeParticipant("call-1", "bob")).expectNext(call).verifyComplete();
    }

    @Test
    void end_shouldStoreTheStatusByName_andBeEmptyOnceAlreadyEnded() {
        when(repository.end("call-1", "MISSED", NOW)).thenReturn(Mono.just(0L));

        StepVerifier.create(adapter.end("call-1", CallStatus.MISSED, NOW)).verifyComplete();
    }

    @Test
    void finders_shouldMapEveryCall() {
        Call call = call(CallStatus.RINGING);
        when(repository.findRingingStartedBefore(NOW)).thenReturn(Flux.just(mapper.toEntity(call)));
        when(repository.findLiveWithParticipantSeenBefore(NOW)).thenReturn(Flux.just(mapper.toEntity(call)));
        when(repository.findLiveStartedBefore(NOW)).thenReturn(Flux.<CallEntity>empty());

        StepVerifier.create(adapter.findRingingStartedBefore(NOW)).expectNext(call).verifyComplete();
        StepVerifier.create(adapter.findLiveWithParticipantSeenBefore(NOW)).expectNext(call).verifyComplete();
        StepVerifier.create(adapter.findLiveStartedBefore(NOW)).verifyComplete();
    }

    private static boolean storeUnavailable(Throwable error) {
        return error instanceof ChatException chat && chat.getErrorCode() == ChatErrorCode.STORE_UNAVAILABLE;
    }

    @Test
    void findLiveInvolving_shouldMapTheCalls() {
        Call call = call(CallStatus.ACTIVE, bob());
        when(repository.findLiveInvolving("bob")).thenReturn(Flux.just(mapper.toEntity(call)));

        StepVerifier.create(adapter.findLiveInvolving("bob")).expectNext(call).verifyComplete();
    }

    @Test
    void findLiveInvolving_shouldTranslateAStoreFailure() {
        when(repository.findLiveInvolving("bob"))
                .thenReturn(Flux.error(new DataAccessResourceFailureException("mongo down")));

        StepVerifier.create(adapter.findLiveInvolving("bob"))
                .expectErrorMatches(MongoCallRepositoryAdapterTest::storeUnavailable)
                .verify();
    }

    @Test
    void removeFromEndedCalls_shouldDelegate() {
        when(repository.pullFromEnded("bob")).thenReturn(Mono.just(4L));

        StepVerifier.create(adapter.removeFromEndedCalls("bob")).verifyComplete();

        verify(repository).pullFromEnded("bob");
    }

    @Test
    void removeFromEndedCalls_shouldTranslateAStoreFailure() {
        when(repository.pullFromEnded("bob"))
                .thenReturn(Mono.error(new DataAccessResourceFailureException("mongo down")));

        StepVerifier.create(adapter.removeFromEndedCalls("bob"))
                .expectErrorMatches(MongoCallRepositoryAdapterTest::storeUnavailable)
                .verify();
    }
}
