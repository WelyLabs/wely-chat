package com.calendar.chat.infrastructure.calls.models;

import java.util.List;

/**
 * Any Realtime answer: each call fills the fields it has. Realtime can answer 200 with an
 * {@code errorCode}, so success is read from the body, not only the status.
 */
public record RealtimeResponse(String sessionId, Boolean requiresImmediateRenegotiation,
                               RealtimeSessionDescription sessionDescription, List<RealtimeTrackResult> tracks,
                               String errorCode, String errorDescription) {}
