package com.calendar.chat.infrastructure.calls.models;

/** A track in Realtime's answer; {@code errorCode} is set when that one track failed. */
public record RealtimeTrackResult(String mid, String trackName, String sessionId, String errorCode,
                                  String errorDescription) {}
