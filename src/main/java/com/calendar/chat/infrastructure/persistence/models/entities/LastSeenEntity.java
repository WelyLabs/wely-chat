package com.calendar.chat.infrastructure.persistence.models.entities;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

/** When a user was last seen connected, kept after their connections are gone. */
@Document(collection = "last_seen")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class LastSeenEntity {

    /** The user id. */
    @Id
    private String id;

    private Instant lastSeenAt;
}
