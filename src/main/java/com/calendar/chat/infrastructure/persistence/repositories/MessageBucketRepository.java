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
    @Update("{ '$set': { 'messages.$.ph.rm': true, 'messages.$.ph.w': 0, 'messages.$.ph.h': 0 }, '$unset': { 'messages.$.ph.k': '' } }")
    Mono<Long> markPhotoRemoved(String conversationId, String messageId);

    /** Whether any bucket of the conversation holds a message with this id. */
    @Query(value = "{ 'conversationId': ?0, 'messages.m_id': ?1 }", exists = true)
    Mono<Boolean> existsWithMessage(String conversationId, String messageId);

    /** The bucket holding the message, projected down to that one message. */
    @Query(value = "{ 'conversationId': ?0, 'messages.m_id': ?1 }", fields = "{ 'conversationId': 1, 'bucketIndex': 1, 'messages.$': 1 }")
    Mono<MessageBucketEntity> findWithMessage(String conversationId, String messageId);

    Mono<Long> deleteByConversationId(String conversationId);

    /** Clears the user's sender name in every bucket still naming them, one atomic update per bucket. */
    @Query("{ 'messages': { '$elemMatch': { 's_id': ?0, 's_un': { '$ne': null } } } }")
    @Update(pipeline = { "{ '$set': { 'messages': { '$map': { 'input': '$messages', 'as': 'm', 'in': { '$cond': [ { '$eq': [ '$$m.s_id', ?0 ] }, { '$mergeObjects': [ '$$m', { 's_un': null } ] }, '$$m' ] } } } } }" })
    Mono<Long> clearSenderName(String userId);
}
