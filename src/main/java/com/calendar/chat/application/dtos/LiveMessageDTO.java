package com.calendar.chat.application.dtos;

import com.calendar.chat.domain.models.BroadcastKind;
import com.calendar.chat.domain.models.MessageType;
import com.calendar.chat.domain.models.Photo;

import java.time.Instant;

/**
 * A frame of {@code chat.stream}: the message's fields, flat as before, plus {@code kind}.
 * NEW is appended by the client, UPDATED replaces the message with the same id.
 */
public record LiveMessageDTO(
        String id,
        String senderId,
        String senderName,
        String receiverId,
        String conversationId,
        String content,
        Instant timestamp,
        Photo photo,
        /** TEXT, or SYSTEM for a notice the application posted. */
        MessageType type,
        BroadcastKind kind
) {}
