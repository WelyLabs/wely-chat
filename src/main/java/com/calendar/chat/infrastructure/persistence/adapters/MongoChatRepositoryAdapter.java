package com.calendar.chat.infrastructure.persistence.adapters;

import com.calendar.chat.domain.models.ConversationDetail;
import com.calendar.chat.domain.models.ConversationSummary;
import com.calendar.chat.domain.models.Message;
import com.calendar.chat.domain.models.MessageBucket;
import com.calendar.chat.domain.ports.ChatRepository;
import com.calendar.chat.infrastructure.persistence.mappers.ConversationMapper;
import com.calendar.chat.infrastructure.persistence.models.dtos.MessageEntity;
import com.calendar.chat.infrastructure.persistence.models.entities.ConversationEntity;
import com.calendar.chat.infrastructure.persistence.models.entities.MessageBucketEntity;
import com.calendar.chat.infrastructure.persistence.repositories.ConversationRepository;
import com.calendar.chat.infrastructure.persistence.repositories.MessageBucketRepository;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static com.calendar.chat.infrastructure.persistence.models.entities.MessageBucketEntity.MAX_MESSAGES;

@Component
public class MongoChatRepositoryAdapter implements ChatRepository {

    private final ConversationRepository conversationRepository;
    private final MessageBucketRepository messageBucketRepository;
    private final ConversationMapper conversationMapper;
    private final ReactiveMongoTemplate mongoTemplate;

    public MongoChatRepositoryAdapter(ConversationRepository conversationRepository,
                                      MessageBucketRepository messageBucketRepository,
                                      ConversationMapper conversationMapper,
                                      ReactiveMongoTemplate mongoTemplate) {
        this.conversationRepository = conversationRepository;
        this.messageBucketRepository = messageBucketRepository;
        this.conversationMapper = conversationMapper;
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public Mono<ConversationDetail> saveWithInitialBucket(List<String> participantIds) {

        ConversationEntity conversationEntityToSave =
                new ConversationEntity(null, participantIds, null, LocalDateTime.now());

        return conversationRepository.save(conversationEntityToSave)
                .flatMap(conversationEntity -> {
                    MessageBucketEntity messageBucketEntityToSave = new MessageBucketEntity(
                            null,
                            conversationEntity.getId(),
                            0,
                            new ArrayList<>()
                    );

                    ConversationDetail conversationDetail =
                            conversationMapper.toConversationDetail(conversationEntity);

                    return messageBucketRepository.save(messageBucketEntityToSave)
                            .map(messageBucketEntity -> {
                                conversationDetail.setMessages(messageBucketEntity.getMessages().stream()
                                        .map(conversationMapper::toMessage)
                                        .toList());
                                conversationDetail.setBucketIndex(messageBucketEntity.getBucketIndex());
                                return conversationDetail;
                            });
                });
    }

    @Override
    public Mono<ConversationDetail> findByParticipantIds(List<String> participantIds) {
        return conversationRepository.findByParticipantIds(participantIds)
                .flatMap(this::loadConversationWithMessages);
    }

    @Override
    public Mono<ConversationDetail> findById(String conversationId, String requesterId) {
        return conversationRepository.findByIdAndParticipantIdsContaining(conversationId, requesterId)
                .flatMap(this::loadConversationWithMessages);
    }

    /**
     * A conversation whose initial bucket is missing — the insert failed after the
     * conversation was created, and there is no transaction spanning the two — reads
     * as an empty conversation rather than vanishing from the API.
     */
    private Mono<ConversationDetail> loadConversationWithMessages(ConversationEntity conversationEntity) {
        return messageBucketRepository
                .findFirstByConversationIdOrderByBucketIndexDesc(conversationEntity.getId())
                .defaultIfEmpty(new MessageBucketEntity(null, conversationEntity.getId(), 0, List.of()))
                .map(bucket -> buildConversationDetail(conversationEntity, bucket));
    }

    private ConversationDetail buildConversationDetail(ConversationEntity conversationEntity,
                                                       MessageBucketEntity messageBucketEntity) {
        ConversationDetail conversationDetail = conversationMapper.toConversationDetail(conversationEntity);

        List<Message> messages = messageBucketEntity.getMessages().stream()
                .map(conversationMapper::toMessage)
                .toList();

        conversationDetail.setMessages(messages);
        conversationDetail.setBucketIndex(messageBucketEntity.getBucketIndex());

        return conversationDetail;
    }

    /**
     * Appends a message to its conversation's latest bucket.
     *
     * <p>Scoped by sender: a conversation the sender does not take part in yields an
     * empty result, so nothing is written.
     *
     * <p>Every write here is a targeted MongoDB update rather than a read-modify-save
     * of the whole document. Saving the document back would lose a concurrent write:
     * two senders reading the same bucket, each appending in memory, the second save
     * overwriting the first message.
     */
    @Override
    public Mono<Void> postMessage(Message message) {
        MessageEntity messageEntity = conversationMapper.toMessageEntity(message);

        return conversationRepository
                .findByIdAndParticipantIdsContaining(message.conversationId(), message.senderId())
                .flatMap(conversation -> appendToLatestBucket(message.conversationId(), messageEntity)
                        .then(touchConversation(message.conversationId(), messageEntity)))
                .then();
    }

    /**
     * Pushes the message into the newest bucket, rolling over to a new one when full.
     *
     * <p>The push is conditioned on the bucket still having room ({@code messages.49}
     * absent means at most 49 entries), so a bucket that filled up between the read
     * and the write matches nothing instead of overflowing. When nothing matched, the
     * next bucket is created — and if a concurrent sender created it first, the unique
     * index rejects the insert and the whole append is retried against the bucket that
     * now exists.
     */
    private Mono<Void> appendToLatestBucket(String conversationId, MessageEntity messageEntity) {
        return messageBucketRepository.findFirstByConversationIdOrderByBucketIndexDesc(conversationId)
                .flatMap(latest -> pushInto(conversationId, latest.getBucketIndex(), messageEntity)
                        .flatMap(appended -> Boolean.TRUE.equals(appended)
                                ? Mono.empty()
                                : createNextBucket(conversationId, latest.getBucketIndex() + 1, messageEntity)))
                .onErrorResume(DuplicateKeyException.class,
                        e -> appendToLatestBucket(conversationId, messageEntity))
                .then();
    }

    /** @return {@code true} if the bucket had room and the message was appended. */
    private Mono<Boolean> pushInto(String conversationId, int bucketIndex, MessageEntity messageEntity) {
        Query hasRoom = Query.query(Criteria.where("conversationId").is(conversationId)
                .and("bucketIndex").is(bucketIndex)
                .and("messages." + (MAX_MESSAGES - 1)).exists(false));

        return mongoTemplate
                .updateFirst(hasRoom, new Update().push("messages", messageEntity), MessageBucketEntity.class)
                .map(result -> result.getModifiedCount() > 0);
    }

    private Mono<Void> createNextBucket(String conversationId, int bucketIndex, MessageEntity messageEntity) {
        MessageBucketEntity next = new MessageBucketEntity(
                null, conversationId, bucketIndex, new ArrayList<>(List.of(messageEntity)));

        return mongoTemplate.insert(next).then();
    }

    /** Updates the conversation's preview fields without rewriting the whole document. */
    private Mono<Void> touchConversation(String conversationId, MessageEntity messageEntity) {
        Update update = new Update()
                .set("lastMessage", messageEntity)
                .set("updatedAt", LocalDateTime.now());

        return mongoTemplate
                .updateFirst(Query.query(Criteria.where("_id").is(conversationId)),
                        update, ConversationEntity.class)
                .then();
    }

    @Override
    public Mono<MessageBucket> findBucketByConversationIdAndBucketIndex(String conversationId,
                                                                        Integer bucketIndex,
                                                                        String requesterId) {
        // Buckets carry no participant list, so participation is established on
        // the conversation first; the bucket is only read once that check passes.
        return conversationRepository.findByIdAndParticipantIdsContaining(conversationId, requesterId)
                .flatMap(conversation -> messageBucketRepository
                        .findByConversationIdAndBucketIndex(conversationId, bucketIndex))
                .map(conversationMapper::toMessageBucket);
    }

    @Override
    public Flux<ConversationSummary> findUserConversations(String userId) {
        return conversationRepository.findByParticipantIdsContaining(userId)
                .map(conversationMapper::toConversationSummary);
    }
}
