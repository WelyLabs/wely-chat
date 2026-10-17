package com.calendar.chat.domain.models;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * An audio or video call in a conversation, direct or an event's group.
 *
 * @param startedByName the caller's display name, for ringing screens; null on calls stored before it existed
 * @param memberIds    the conversation's members when the call started: who it rings and who can join
 * @param participants who is in the call right now
 * @param endedAt      null while the call is live
 */
public record Call(String id, String conversationId, String startedBy, String startedByName, CallMedia media,
                   CallStatus status,
                   Instant startedAt, Instant endedAt, List<String> memberIds, List<CallParticipant> participants) {

    public Call {
        memberIds = List.copyOf(memberIds);
        participants = List.copyOf(participants);
    }

    /** A call that starts ringing, with nobody in it yet: the caller joins like everyone else. */
    public static Call ringing(String conversationId, String startedBy, String startedByName, CallMedia media,
                               Instant at, List<String> memberIds) {
        return new Call(null, conversationId, startedBy, startedByName, media, CallStatus.RINGING, at, null,
                memberIds, List.of());
    }

    public boolean isLive() {
        return status.isLive();
    }

    public Optional<CallParticipant> participant(String userId) {
        return participants.stream().filter(participant -> participant.userId().equals(userId)).findFirst();
    }

    /** Everyone in the call. */
    public List<String> participantIds() {
        return participants.stream().map(CallParticipant::userId).toList();
    }

    /** Everyone in the call but {@code userId}. */
    public List<String> participantIdsExcept(String userId) {
        return participants.stream().map(CallParticipant::userId).filter(id -> !id.equals(userId)).toList();
    }

    /** Every member but {@code userId}. */
    public List<String> memberIdsExcept(String userId) {
        return memberIds.stream().filter(id -> !id.equals(userId)).toList();
    }
}
