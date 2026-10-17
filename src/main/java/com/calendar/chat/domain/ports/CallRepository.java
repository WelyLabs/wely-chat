package com.calendar.chat.domain.ports;

import com.calendar.chat.domain.models.Call;
import com.calendar.chat.domain.models.CallParticipant;
import com.calendar.chat.domain.models.CallStatus;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;

/**
 * Where calls are kept, shared by every chat instance. Every write is atomic: two people
 * joining at once must both end up in the call.
 */
public interface CallRepository {

    /**
     * Saves a new call, unless the conversation already has a live one. Either way, emits the
     * conversation's live call: the new one, or the one started concurrently.
     */
    Mono<Call> createOrGetLive(Call call);

    /** The conversation's live call; empty when there is none. */
    Mono<Call> findLive(String conversationId);

    /** A call {@code requesterId} was a member of; empty for anyone else, as for an unknown id. */
    Mono<Call> findById(String callId, String requesterId);

    /**
     * Puts a participant in a live call, replacing their previous entry if they were already in.
     * Empty when the call is no longer live, or already holds {@code maxParticipants} others.
     */
    Mono<Call> addParticipant(String callId, CallParticipant participant, int maxParticipants);

    /** Moves a ringing call to ACTIVE; emits the call either way. */
    Mono<Call> markActive(String callId);

    /** Adds tracks to what a participant publishes; empty when they are not in the call. */
    Mono<Call> addTracks(String callId, String userId, List<String> trackNames);

    /** Records a participant's heartbeat; empty when they are not in the call. */
    Mono<Call> touch(String callId, String userId, Instant at);

    /** Takes a participant out of the call; empty when they were not in it. */
    Mono<Call> removeParticipant(String callId, String userId);

    /** Ends a live call with a final status; empty when it had already ended. */
    Mono<Call> end(String callId, CallStatus status, Instant at);

    /** Calls still ringing that started before {@code startedBefore}. */
    Flux<Call> findRingingStartedBefore(Instant startedBefore);

    /** Live calls with at least one participant last seen before {@code seenBefore}. */
    Flux<Call> findLiveWithParticipantSeenBefore(Instant seenBefore);

    /** Live calls that started before {@code startedBefore}. */
    Flux<Call> findLiveStartedBefore(Instant startedBefore);

    /** Live calls the user is in or started, and ringing calls the user is a member of. */
    Flux<Call> findLiveInvolving(String userId);

    /**
     * Takes the user's participant entries (session, tracks) out of every ended call, and their
     * name out of every call they started.
     */
    Mono<Void> removeFromEndedCalls(String userId);
}
