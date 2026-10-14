package com.calendar.chat.infrastructure.calls.models;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

/** Body of {@code tracks/new} and {@code tracks/update}; publishing also carries the browser's offer. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record RealtimeTracksRequest(RealtimeSessionDescription sessionDescription, List<RealtimeTrack> tracks) {}
