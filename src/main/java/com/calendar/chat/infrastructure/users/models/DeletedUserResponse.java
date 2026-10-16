package com.calendar.chat.infrastructure.users.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.util.UUID;

/** One tombstone of wely-users' internal listing; a UUID type, so anything but a UUID makes the body unreadable. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record DeletedUserResponse(UUID userId, Instant deletedAt) {}
