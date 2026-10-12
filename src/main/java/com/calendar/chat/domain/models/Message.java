package com.calendar.chat.domain.models;

import java.time.Instant;

public record Message(
        String id,
        String senderId,
        String senderName,
        String receiverId,
        String conversationId,
        String content,
        Instant timestamp
) {}
