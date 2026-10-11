package com.calendar.chat.infrastructure.persistence.models.entities;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

/** One open chat stream: who, on which instance, last refreshed when. */
@Document(collection = "presence_connections")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class PresenceConnectionEntity {

    @Id
    private String id;

    @Indexed
    private String userId;

    private String instanceId;

    /**
     * TTL: MongoDB deletes a connection two minutes after its last refresh, which is how the
     * connections of an instance that crashed without closing them go away.
     */
    @Indexed(expireAfter = "2m")
    private Instant seenAt;
}
