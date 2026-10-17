package com.calendar.chat.infrastructure.messaging.models;

import java.time.Instant;
import java.util.List;

/**
 * A call event on the broker, addressed to one person. Kept apart from the domain on purpose: it
 * is the contract between instances, which may run different versions during a rollout.
 */
public record CallEventBroadcastDTO(String type, String recipientId, String actorId, CallDTO call) {

    /** {@code startedByName} is null from an instance that predates it. */
    public record CallDTO(String id, String conversationId, String startedBy, String startedByName, String media,
                          String status,
                          Instant startedAt, Instant endedAt, List<String> memberIds,
                          List<ParticipantDTO> participants) {}

    public record ParticipantDTO(String userId, String sessionId, Instant joinedAt, Instant lastSeenAt,
                                 List<String> trackNames) {}
}
