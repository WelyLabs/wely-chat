package com.calendar.chat.infrastructure.calls.models;

/** Body of {@code generate-ice-servers}: how long the credentials last, in seconds. */
public record TurnCredentialsRequest(long ttl) {}
