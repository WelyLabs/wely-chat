package com.calendar.chat.domain.models;

import java.time.Instant;

/** The permanent marker of a deleted account; {@code erasedAt} stays null until the erasure has run once. */
public record DeletedUserMarker(String userId, Instant deletedAt, Instant erasedAt) {
}
