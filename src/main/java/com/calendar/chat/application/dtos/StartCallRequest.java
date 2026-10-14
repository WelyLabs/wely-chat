package com.calendar.chat.application.dtos;

/** Body of {@code POST /calls}. {@code media} is AUDIO or VIDEO. */
public record StartCallRequest(String conversationId, String media) {}
