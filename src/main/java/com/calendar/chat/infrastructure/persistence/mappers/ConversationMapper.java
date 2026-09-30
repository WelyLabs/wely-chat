package com.calendar.chat.infrastructure.persistence.mappers;

import com.calendar.chat.domain.models.ConversationDetail;
import com.calendar.chat.domain.models.ConversationSummary;
import com.calendar.chat.domain.models.Message;
import com.calendar.chat.domain.models.MessageBucket;
import com.calendar.chat.infrastructure.persistence.models.dtos.MessageEntity;
import com.calendar.chat.infrastructure.persistence.models.entities.ConversationEntity;
import com.calendar.chat.infrastructure.persistence.models.entities.MessageBucketEntity;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/**
 * Translates between the domain and what MongoDB stores.
 *
 * <p>{@code unmappedTargetPolicy = ERROR} is the point of this configuration. The two sides
 * deliberately use different field names — the entities abbreviate them, because MongoDB
 * repeats every key in every document — and MapStruct maps by name, so a field whose names
 * differ is silently dropped. That is exactly what happened to the message id: {@code id} on
 * one side, {@code messageId} on the other, so every message was written with a null id and
 * read back without one. Nothing failed; the id simply was not there.
 *
 * <p>With the policy set to ERROR, every target field is now either mapped or ignored in
 * writing, and adding a field to either side stops the build until someone decides which.
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface ConversationMapper {

    @Mapping(target = "participantIds", ignore = true)
    ConversationEntity toConversationEntity(ConversationSummary conversationSummary);

    @Mapping(target = "type", ignore = true)
    @Mapping(target = "title", ignore = true)
    ConversationSummary toConversationSummary(ConversationEntity conversationEntity);

    @Mapping(target = "type", ignore = true)
    @Mapping(target = "messages", ignore = true)
    @Mapping(target = "bucketIndex", ignore = true)
    ConversationDetail toConversationDetail(ConversationEntity conversationEntity);

    /**
     * The recipient and the conversation are not stored on a message: a message lives inside a
     * bucket already keyed by conversation, and the recipient is a participant of it. Repeating
     * either on all fifty messages of a document would be the bucket pattern working against
     * itself.
     */
    @Mapping(target = "id", source = "messageId")
    @Mapping(target = "receiverId", ignore = true)
    @Mapping(target = "conversationId", ignore = true)
    Message toMessage(MessageEntity messageEntity);

    @Mapping(target = "messageId", source = "id")
    MessageEntity toMessageEntity(Message message);

    @Mapping(target = "messages", source = "messages")
    MessageBucket toMessageBucket(MessageBucketEntity messageBucketEntity);
}
