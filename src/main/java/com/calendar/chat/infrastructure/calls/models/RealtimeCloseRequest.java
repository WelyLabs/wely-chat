package com.calendar.chat.infrastructure.calls.models;

import java.util.List;

/** Body of {@code tracks/close}. {@code force} closes without a new offer from the browser. */
public record RealtimeCloseRequest(List<RealtimeTrack> tracks, boolean force) {}
