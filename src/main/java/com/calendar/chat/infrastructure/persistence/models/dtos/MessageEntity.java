package com.calendar.chat.infrastructure.persistence.models.dtos;

import org.springframework.data.mongodb.core.mapping.Field;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * A message as it sits inside a bucket.
 *
 * <p>The field names are abbreviated because MongoDB stores every key in every document, and a
 * bucket holds fifty of these. The domain record keeps readable names; the mapper bridges the
 * two, with {@code unmappedTargetPolicy = ERROR} so a field cannot be dropped silently — which
 * is exactly what happened to the message id before.
 *
 * <p>Type, reactions and attachments are not stored: {@code MessageType} exists in the domain
 * and nothing writes anything but TEXT. They belong here the day a feature needs them, not as
 * commented-out fields.
 */
public record MessageEntity(
        @Field("m_id") String messageId,
        @Field("s_id") String senderId,
        @Field("s_un") String senderName,
        @Field("txt") String content,
        @Field("ts") LocalDateTime timestamp
) {}