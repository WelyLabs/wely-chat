package com.calendar.chat.infrastructure.calls.models;

import java.util.List;

/** Answer of {@code generate-ice-servers}. */
public record TurnCredentialsResponse(List<TurnIceServer> iceServers) {}
