package com.calendar.chat.infrastructure.media.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** The part of wely-media's RFC 7807 error the chat branches on. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record MediaProblem(String code) {}
