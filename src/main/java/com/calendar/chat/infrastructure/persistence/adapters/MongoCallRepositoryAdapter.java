package com.calendar.chat.infrastructure.persistence.adapters;

import com.calendar.chat.domain.models.Call;
import com.calendar.chat.domain.models.CallParticipant;
import com.calendar.chat.domain.models.CallStatus;
import com.calendar.chat.domain.ports.CallRepository;
import com.calendar.chat.infrastructure.persistence.mappers.CallPersistenceMapper;
import com.calendar.chat.infrastructure.persistence.repositories.CallMongoRepository;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;

/**
 * Calls in MongoDB. Each write is one conditional update; the call is read back afterwards, so
 * what comes back may already include a later change by someone else, never an older state.
 */
@Component
public class MongoCallRepositoryAdapter implements CallRepository {

    private final CallMongoRepository repository;
    private final CallPersistenceMapper mapper;

    public MongoCallRepositoryAdapter(CallMongoRepository repository, CallPersistenceMapper mapper) {
        this.repository = repository;
        this.mapper = mapper;
    }

    /** The unique index on live calls rejects the second of two concurrent starts. */
    @Override
    public Mono<Call> createOrGetLive(Call call) {
        return repository.save(mapper.toEntity(call))
                .map(mapper::toDomain)
                .onErrorResume(DuplicateKeyException.class, alreadyLive -> findLive(call.conversationId()));
    }

    @Override
    public Mono<Call> findLive(String conversationId) {
        return repository.findByConversationIdAndLiveTrue(conversationId).map(mapper::toDomain);
    }

    @Override
    public Mono<Call> findById(String callId, String requesterId) {
        return repository.findMemberCall(callId, requesterId).map(mapper::toDomain);
    }

    /** A rejoin first drops the participant's previous entry, then takes a place if one is left. */
    @Override
    public Mono<Call> addParticipant(String callId, CallParticipant participant, int maxParticipants) {
        return repository.pullFromLive(callId, participant.userId())
                .then(repository.pushIfRoom(callId, mapper.toEntity(participant), maxParticipants))
                .flatMap(changed -> readIfChanged(callId, changed));
    }

    @Override
    public Mono<Call> markActive(String callId) {
        return repository.markActive(callId).then(read(callId));
    }

    @Override
    public Mono<Call> addTracks(String callId, String userId, List<String> trackNames) {
        return repository.addTracks(callId, userId, trackNames).flatMap(changed -> readIfChanged(callId, changed));
    }

    @Override
    public Mono<Call> touch(String callId, String userId, Instant at) {
        return repository.touch(callId, userId, at).flatMap(changed -> readIfChanged(callId, changed));
    }

    @Override
    public Mono<Call> removeParticipant(String callId, String userId) {
        return repository.removeParticipant(callId, userId).flatMap(changed -> readIfChanged(callId, changed));
    }

    @Override
    public Mono<Call> end(String callId, CallStatus status, Instant at) {
        return repository.end(callId, status.name(), at).flatMap(changed -> readIfChanged(callId, changed));
    }

    @Override
    public Flux<Call> findRingingStartedBefore(Instant startedBefore) {
        return repository.findRingingStartedBefore(startedBefore).map(mapper::toDomain);
    }

    @Override
    public Flux<Call> findLiveWithParticipantSeenBefore(Instant seenBefore) {
        return repository.findLiveWithParticipantSeenBefore(seenBefore).map(mapper::toDomain);
    }

    @Override
    public Flux<Call> findLiveStartedBefore(Instant startedBefore) {
        return repository.findLiveStartedBefore(startedBefore).map(mapper::toDomain);
    }

    @Override
    public Flux<Call> findLiveInvolving(String userId) {
        return repository.findLiveInvolving(userId)
                .map(mapper::toDomain)
                .onErrorMap(DataAccessException.class, StoreErrors::unavailable);
    }

    @Override
    public Mono<Void> removeFromEndedCalls(String userId) {
        return repository.pullFromEnded(userId)
                .onErrorMap(DataAccessException.class, StoreErrors::unavailable)
                .then();
    }

    private Mono<Call> readIfChanged(String callId, long changed) {
        return changed > 0 ? read(callId) : Mono.empty();
    }

    private Mono<Call> read(String callId) {
        return repository.findById(callId).map(mapper::toDomain);
    }
}
