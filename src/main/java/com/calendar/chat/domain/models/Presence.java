package com.calendar.chat.domain.models;

import java.time.Instant;

/**
 * Whether a user is connected right now, and when they were last seen.
 *
 * @param lastSeenAt null when the user has never opened the chat
 */
public record Presence(String userId, boolean online, Instant lastSeenAt) {
}
