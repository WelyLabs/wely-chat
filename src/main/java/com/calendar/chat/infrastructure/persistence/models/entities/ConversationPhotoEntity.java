package com.calendar.chat.infrastructure.persistence.models.entities;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

/** One photo of a conversation, for its media tab: newest first, one entry per message. */
@Document(collection = "conversation_photos")
@CompoundIndexes({
        @CompoundIndex(name = "conversation_sent", def = "{'conversationId': 1, 'sentAt': -1}"),
        @CompoundIndex(name = "conversation_message_unique", def = "{'conversationId': 1, 'messageId': 1}", unique = true)
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ConversationPhotoEntity {
    @Id
    private String id;
    private String conversationId;
    private String messageId;
    private String senderId;
    private String key;
    private int width;
    private int height;
    private Instant sentAt;
}
