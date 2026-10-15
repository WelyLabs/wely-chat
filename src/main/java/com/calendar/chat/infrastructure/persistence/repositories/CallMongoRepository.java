package com.calendar.chat.infrastructure.persistence.repositories;

import com.calendar.chat.infrastructure.persistence.models.entities.CallEntity;
import com.calendar.chat.infrastructure.persistence.models.entities.CallParticipantEntity;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.data.mongodb.repository.Update;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;

/**
 * Every write is conditioned on the call still being live, and returns how many documents it
 * changed: 0 means the condition no longer held, and nothing was written.
 */
public interface CallMongoRepository extends ReactiveMongoRepository<CallEntity, String> {

    Mono<CallEntity> findByConversationIdAndLiveTrue(String conversationId);

    /** Scoped to the members, like conversations are: someone else's call is not found. */
    @Query("{ '_id': ?0, 'memberIds': ?1 }")
    Mono<CallEntity> findMemberCall(String callId, String userId);

    @Query("{ '_id': ?0, 'live': true }")
    @Update("{ '$pull': { 'participants': { 'userId': ?1 } } }")
    Mono<Long> pullFromLive(String callId, String userId);

    /** The size check and the push are one write: two people cannot take the last place. */
    @Query("{ '_id': ?0, 'live': true, $expr: { $lt: [ { $size: '$participants' }, ?2 ] } }")
    @Update("{ '$push': { 'participants': ?1 } }")
    Mono<Long> pushIfRoom(String callId, CallParticipantEntity participant, int maxParticipants);

    @Query("{ '_id': ?0, 'status': 'RINGING' }")
    @Update("{ '$set': { 'status': 'ACTIVE' } }")
    Mono<Long> markActive(String callId);

    @Query("{ '_id': ?0, 'live': true, 'participants.userId': ?1 }")
    @Update("{ '$addToSet': { 'participants.$.trackNames': { $each: ?2 } } }")
    Mono<Long> addTracks(String callId, String userId, List<String> trackNames);

    @Query("{ '_id': ?0, 'live': true, 'participants.userId': ?1 }")
    @Update("{ '$set': { 'participants.$.lastSeenAt': ?2 } }")
    Mono<Long> touch(String callId, String userId, Instant at);

    @Query("{ '_id': ?0, 'live': true, 'participants.userId': ?1 }")
    @Update("{ '$pull': { 'participants': { 'userId': ?1 } } }")
    Mono<Long> removeParticipant(String callId, String userId);

    @Query("{ '_id': ?0, 'live': true }")
    @Update("{ '$set': { 'status': ?1, 'live': false, 'endedAt': ?2 } }")
    Mono<Long> end(String callId, String status, Instant at);

    @Query("{ 'live': true, 'status': 'RINGING', 'startedAt': { $lt: ?0 } }")
    Flux<CallEntity> findRingingStartedBefore(Instant startedBefore);

    @Query("{ 'live': true, 'participants.lastSeenAt': { $lt: ?0 } }")
    Flux<CallEntity> findLiveWithParticipantSeenBefore(Instant seenBefore);

    @Query("{ 'live': true, 'startedAt': { $lt: ?0 } }")
    Flux<CallEntity> findLiveStartedBefore(Instant startedBefore);

    /** Live calls the user is in or started, and ringing calls they are a member of. */
    @Query("{ 'live': true, $or: [ { 'participants.userId': ?0 }, { 'startedBy': ?0 },"
            + " { 'status': 'RINGING', 'memberIds': ?0 } ] }")
    Flux<CallEntity> findLiveInvolving(String userId);

    /** Ended calls are never read again; this only drops the user's session entries. */
    @Query("{ 'live': false, 'participants.userId': ?0 }")
    @Update("{ '$pull': { 'participants': { 'userId': ?0 } } }")
    Mono<Long> pullFromEnded(String userId);
}
