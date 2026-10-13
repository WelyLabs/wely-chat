package com.calendar.chat.infrastructure.persistence.models.dtos;

import org.springframework.data.mongodb.core.mapping.Field;

import java.time.Instant;
import java.util.Map;

/**
 * A message as it sits inside a bucket.
 *
 * <p>The field names are abbreviated because MongoDB stores every key in every document, and a
 * bucket holds fifty of these. The domain record keeps readable names; the mapper bridges the
 * two, with {@code unmappedTargetPolicy = ERROR} so a field cannot be dropped silently — which
 * is exactly what happened to the message id before.
 *
 * <p>A photo is stored as its key and size, never as a URL: URLs are signed when a message is
 * read, and expire. Reactions are not stored: they belong here the day a feature needs them.
 */
public record MessageEntity(
        @Field("m_id") String messageId,
        @Field("s_id") String senderId,
        @Field("s_un") String senderName,
        @Field("txt") String content,
        @Field("ts") Instant timestamp,
        /** Absent on a text message. */
        @Field("ph") PhotoEntity photo
) {}
