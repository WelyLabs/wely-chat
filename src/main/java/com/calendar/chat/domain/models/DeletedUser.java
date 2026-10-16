package com.calendar.chat.domain.models;

import java.time.Instant;
import java.util.Objects;

/** An account wely-users has deleted, as its tombstone lists it; a tombstone always names its account. */
public record DeletedUser(String userId, Instant deletedAt) {

    public DeletedUser {
        Objects.requireNonNull(userId, "userId");
    }
}
