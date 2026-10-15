package com.calendar.chat.infrastructure.persistence.repositories;

import com.calendar.chat.infrastructure.persistence.models.entities.ConversationPhotoEntity;
import org.springframework.data.domain.Limit;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;

/** Every query names the conversation. */
public interface ConversationPhotoMongoRepository extends ReactiveMongoRepository<ConversationPhotoEntity, String> {

    Flux<ConversationPhotoEntity> findByConversationIdOrderBySentAtDesc(String conversationId, Limit limit);

    Flux<ConversationPhotoEntity> findByConversationIdAndSentAtBeforeOrderBySentAtDesc(String conversationId, Instant before,
                                                                                       Limit limit);

    Mono<ConversationPhotoEntity> findByConversationIdAndMessageId(String conversationId, String messageId);

    Mono<Long> deleteByConversationIdAndMessageId(String conversationId, String messageId);

    Flux<ConversationPhotoEntity> findByConversationId(String conversationId);

    Mono<Long> deleteByConversationId(String conversationId);
}
