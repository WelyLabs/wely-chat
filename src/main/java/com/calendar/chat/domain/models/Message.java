package com.calendar.chat.domain.models;

import java.time.Instant;

/** @param type what the message carries; never null, TEXT unless said otherwise. */
public record Message(
        String id,
        String senderId,
        String senderName,
        String receiverId,
        String conversationId,
        String content,
        Instant timestamp,
        Photo photo,
        MessageType type
) {
    /** Who signs a notice posted by the application itself; no member has this id. */
    public static final String SYSTEM_SENDER_ID = "system";
    public static final String SYSTEM_SENDER_NAME = "Wely";

    /** A message stored before types existed carries none: it is a text one. */
    public Message {
        type = type == null ? MessageType.TEXT : type;
    }

    /** A text message: most of them, and every one stored before photos existed. */
    public Message(String id, String senderId, String senderName, String receiverId, String conversationId,
                   String content, Instant timestamp) {
        this(id, senderId, senderName, receiverId, conversationId, content, timestamp, null, MessageType.TEXT);
    }

    /** A user's message, with or without a photo. */
    public Message(String id, String senderId, String senderName, String receiverId, String conversationId,
                   String content, Instant timestamp, Photo photo) {
        this(id, senderId, senderName, receiverId, conversationId, content, timestamp, photo, MessageType.TEXT);
    }

    /** A notice the application posts, such as an event's date being fixed; its conversation is set when stored. */
    public static Message system(String id, String content, Instant timestamp) {
        return new Message(id, SYSTEM_SENDER_ID, SYSTEM_SENDER_NAME, null, null, content, timestamp, null,
                MessageType.SYSTEM);
    }

    public boolean hasStoredPhoto() {
        return photo != null && photo.key() != null && !photo.removed();
    }
}
