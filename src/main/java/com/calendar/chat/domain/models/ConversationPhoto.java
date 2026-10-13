package com.calendar.chat.domain.models;

import java.time.Instant;

/** One photo of a conversation, as its media tab lists it: newest first, without reading the messages. */
public record ConversationPhoto(String conversationId, String messageId, String senderId, Photo photo, Instant sentAt) {}
