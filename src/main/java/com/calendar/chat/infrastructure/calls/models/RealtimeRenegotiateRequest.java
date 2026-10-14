package com.calendar.chat.infrastructure.calls.models;

/** Body of {@code renegotiate}: the browser's answer to the relay's offer. */
public record RealtimeRenegotiateRequest(RealtimeSessionDescription sessionDescription) {}
