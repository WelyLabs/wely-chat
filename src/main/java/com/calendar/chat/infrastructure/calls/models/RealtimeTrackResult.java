package com.calendar.chat.infrastructure.calls.models;

/** A track in Realtime's answer; {@code errorCode} marks a failed track, {@code status} comes with a session read. */
public record RealtimeTrackResult(String mid, String trackName, String sessionId, String errorCode,
                                  String errorDescription, String status) {}
