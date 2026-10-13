package com.calendar.chat.infrastructure.media.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** wely-media's answer to {@code POST /internal/media}. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record StoredMediaResponse(String key, int width, int height, String contentType) {}
