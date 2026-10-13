package com.calendar.chat.infrastructure.media.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Map;

/** wely-media's signed URLs, by key. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record SignedUrlsResponse(Map<String, String> urls) {}
