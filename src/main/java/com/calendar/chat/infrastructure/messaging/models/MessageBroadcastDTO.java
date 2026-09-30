package com.calendar.chat.infrastructure.messaging.models;

import java.time.LocalDateTime;

/**
 * A message as it travels over the broker.
 *
 * <p>Separate from the domain {@code Message} on purpose: what goes on a topic is a contract
 * with every consumer, present and future, and renaming a domain field should not break
 * whoever is reading the stream.
 */
public record MessageBroadcastDTO(
        String id,
        String senderId,
        String senderName,
        String receiverId,
        String conversationId,
        String content,
        LocalDateTime timestamp
) {}
