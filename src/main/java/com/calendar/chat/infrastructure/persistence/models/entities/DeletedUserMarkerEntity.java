package com.calendar.chat.infrastructure.persistence.models.entities;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

/** A deleted account's marker, kept permanently; every lookup is by id, so no other index. */
@Document(collection = "deleted_users")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class DeletedUserMarkerEntity {

    /** The businessId. */
    @Id
    private String id;

    private Instant deletedAt;

    /** Absent until every erasure step has run. */
    private Instant erasedAt;
}
