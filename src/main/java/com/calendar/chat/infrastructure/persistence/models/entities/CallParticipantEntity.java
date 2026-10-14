package com.calendar.chat.infrastructure.persistence.models.entities;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;
import java.util.List;

/** Someone in a call, embedded in its {@link CallEntity}. */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class CallParticipantEntity {
    private String userId;
    private String sessionId;
    private Instant joinedAt;
    private Instant lastSeenAt;
    private List<String> trackNames;
}
