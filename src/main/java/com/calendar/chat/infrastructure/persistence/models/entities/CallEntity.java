package com.calendar.chat.infrastructure.persistence.models.entities;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.List;

/**
 * A call, with its participants embedded: one document, so joining and leaving are single
 * atomic updates.
 *
 * <p>{@code live} mirrors the status for the unique index: MongoDB's partial filters take an
 * equality, not the {@code $in} that "RINGING or ACTIVE" would need.
 */
@Document(collection = "calls")
@CompoundIndexes({
        // At most one live call per conversation, even when two members start one at once.
        @CompoundIndex(name = "conversation_live_unique", def = "{'conversationId': 1}", unique = true,
                partialFilter = "{'live': true}"),
        @CompoundIndex(name = "live_started", def = "{'live': 1, 'startedAt': 1}")
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class CallEntity {
    @Id
    private String id;
    private String conversationId;
    private String startedBy;
    /** Absent on calls stored before it existed: read as null. */
    private String startedByName;
    private String media;
    private String status;
    private boolean live;
    private Instant startedAt;
    private Instant endedAt;
    private List<String> memberIds;
    private List<CallParticipantEntity> participants;
}
