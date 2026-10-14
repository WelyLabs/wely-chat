package com.calendar.chat.infrastructure.calls.models;

/** An offer or answer as Cloudflare Realtime exchanges it. */
public record RealtimeSessionDescription(String type, String sdp) {}
