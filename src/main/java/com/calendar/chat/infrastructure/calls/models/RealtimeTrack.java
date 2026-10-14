package com.calendar.chat.infrastructure.calls.models;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * A track in a Realtime request: "local" to publish, "remote" to receive one published on
 * {@code sessionId}. Fields that do not apply are left out of the JSON.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record RealtimeTrack(String location, String mid, String trackName, String sessionId,
                            RealtimeSimulcast simulcast) {}
