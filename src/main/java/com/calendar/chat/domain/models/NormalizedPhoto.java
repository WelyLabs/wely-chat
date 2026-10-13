package com.calendar.chat.domain.models;

/** A photo re-encoded for storage: its bytes, their type and its size in pixels. */
public record NormalizedPhoto(byte[] content, String contentType, int width, int height) {}
