package com.calendar.chat.infrastructure.persistence.models.entities;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

/** A pair of the friendships projection, keyed {@code a:b}; an ended pair stays, not friends, to keep its version. */
@Document(collection = "friendships")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class FriendshipEntity {

    @Id
    private String id;

    /** The lower id; indexed with {@code b} for an account's erasure. */
    @Indexed
    private String a;

    @Indexed
    private String b;

    private boolean friends;

    private long version;
}
