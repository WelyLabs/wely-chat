package com.calendar.chat.infrastructure.persistence.repositories;

import org.springframework.data.mongodb.repository.Update;
import org.springframework.data.mongodb.repository.Query;
import com.calendar.chat.infrastructure.persistence.models.entities.MessageBucketEntity;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
public interface MessageBucketRepository extends ReactiveMongoRepository<MessageBucketEntity, String> {

    Mono<MessageBucketEntity> findFirstByConversationIdOrderByBucketIndexDesc(String conversationId);

    Mono<MessageBucketEntity> findByConversationIdAndBucketIndex(String conversationId, Integer bucketIndex);

    /** The message keeps its place; its photo loses its key and says it was removed. */
    @Query("{ 'conversationId': ?0, 'messages.m_id': ?1 }")
    @Update("{ '$set': { 'messages.$.ph.rm': true }, '$unset': { 'messages.$.ph.k': '' } }")
    Mono<Long> markPhotoRemoved(String conversationId, String messageId);
}
