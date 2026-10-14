package com.calendar.chat.infrastructure.calls.models;

/**
 * Which simulcast layer to forward. "asciibetical" falls back on the next rid when the preferred
 * one is not sent, which happens when the publisher's network drops its higher layers.
 */
public record RealtimeSimulcast(String preferredRid, String priorityOrdering, String ridNotAvailable) {}
