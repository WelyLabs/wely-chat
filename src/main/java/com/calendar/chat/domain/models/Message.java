package com.calendar.chat.domain.models;

import java.time.Instant;

public record Message(
        String id,
        String senderId,
        String senderName,
        String receiverId,
        String conversationId,
        String content,
        Instant timestamp,
        Photo photo
) {
    /** A text message: most of them, and every one stored before photos existed. */
    public Message(String id, String senderId, String senderName, String receiverId, String conversationId,
                   String content, Instant timestamp) {
        this(id, senderId, senderName, receiverId, conversationId, content, timestamp, null);
    }

    public boolean hasStoredPhoto() {
        return photo != null && photo.key() != null && !photo.removed();
    }
}
