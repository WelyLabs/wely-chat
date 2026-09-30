package com.calendar.chat.infrastructure.persistence.models.entities;

import com.calendar.chat.infrastructure.persistence.models.dtos.MessageEntity;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;

/**
 * A slice of a conversation's history — up to {@code MAX_MESSAGES} messages per
 * document (bucket pattern), so loading a conversation reads one document rather
 * than one per message.
 *
 * <p>The unique index on {@code (conversationId, bucketIndex)} is what makes
 * concurrent rollover safe: two writers racing to open the same next bucket, one
 * loses with a duplicate key and retries against the bucket the winner created.
 */
@Document(collection = "message_buckets")
@CompoundIndex(name = "conversation_bucket_unique",
               def = "{'conversationId': 1, 'bucketIndex': -1}",
               unique = true)
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class MessageBucketEntity {

    /** Messages per bucket. Changing this does not require migrating existing buckets. */
    public static final int MAX_MESSAGES = 50;

    @Id
    private String id;
    private String conversationId;
    private Integer bucketIndex;
    private List<MessageEntity> messages;
}
