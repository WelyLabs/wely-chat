package com.calendar.chat.infrastructure.messaging.models;

import java.time.Instant;

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
        Instant timestamp,
        /** Its storage key and size; never a URL, which each pod signs when it delivers the message. */
        PhotoDTO photo,
        /** NEW or UPDATED; absent from records written before updates were pushed, which are all NEW. */
        String kind
) {
    public record PhotoDTO(String key, int width, int height, boolean removed) {}
}
