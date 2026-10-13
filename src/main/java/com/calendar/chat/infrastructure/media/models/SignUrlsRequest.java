package com.calendar.chat.infrastructure.media.models;

import java.util.Collection;

/** Body of {@code POST /internal/media/urls}: at most 200 keys. */
public record SignUrlsRequest(Collection<String> keys, long ttlSeconds) {}
