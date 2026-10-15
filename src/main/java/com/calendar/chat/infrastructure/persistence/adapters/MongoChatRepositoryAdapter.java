package com.calendar.chat.infrastructure.persistence.adapters;

import com.calendar.chat.domain.models.ConversationDetail;
import com.calendar.chat.domain.models.ConversationSummary;
import com.calendar.chat.domain.models.ConversationType;
import com.calendar.chat.domain.models.EventGroup;
import com.calendar.chat.domain.models.Message;
import com.calendar.chat.domain.models.MessageBucket;
import com.calendar.chat.domain.models.PostedMessage;
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

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static com.calendar.chat.infrastructure.persistence.models.entities.MessageBucketEntity.MAX_MESSAGES;

@Component
public class MongoChatRepositoryAdapter implements ChatRepository {

    private static final int NO_BUCKET = -1;

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
                ConversationEntity.direct(null, participantIds, null, Instant.now());

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
    public Mono<List<String>> postMessage(Message message) {
        MessageEntity messageEntity = conversationMapper.toMessageEntity(message);

        return conversationRepository
                .findByIdAndParticipantIdsContaining(message.conversationId(), message.senderId())
                .flatMap(conversation -> appendToLatestBucket(message.conversationId(), messageEntity)
                        .then(touchConversation(message.conversationId(), messageEntity))
                        .thenReturn(List.copyOf(conversation.getParticipantIds())));
    }

    /**
     * Appends a notice to the event's group, unless a message with its id is already stored.
     *
     * <p>Check then write, not atomic: two copies of one record handled at once would both write.
     * Records of a topic partition are handled one at a time, so only a redelivery reaches this,
     * after the first copy is stored.
     */
    @Override
    public Mono<PostedMessage> postSystemMessage(String eventId, Message message) {
        return conversationRepository.findByEventId(eventId)
                .flatMap(conversation -> postInto(conversation, message));
    }

    @Override
    public Mono<PostedMessage> postSystemMessageToConversation(String conversationId, Message message) {
        return conversationRepository.findById(conversationId)
                .flatMap(conversation -> postInto(conversation, message));
    }

    /** Stores an application notice once, keyed by its id; no sender check. */
    private Mono<PostedMessage> postInto(ConversationEntity conversation, Message message) {
        Message stored = new Message(message.id(), message.senderId(), message.senderName(), null,
                conversation.getId(), message.content(), message.timestamp(), message.photo(), message.type());
        List<String> participants = conversation.getParticipantIds() == null
                ? List.of() : List.copyOf(conversation.getParticipantIds());
        return messageBucketRepository.existsWithMessage(conversation.getId(), message.id())
                .flatMap(exists -> exists
                        ? Mono.just(new PostedMessage(stored, participants, true))
                        : Mono.defer(() -> {
                            MessageEntity entity = conversationMapper.toMessageEntity(stored);
                            return appendToLatestBucket(conversation.getId(), entity)
                                    .then(touchConversation(conversation.getId(), entity))
                                    .thenReturn(new PostedMessage(stored, participants, false));
                        }));
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
                .map(MessageBucketEntity::getBucketIndex)
                // No bucket at all — an event group gets its first with its first message. Without
                // this, the flatMap below had nothing to run on and the message vanished.
                .defaultIfEmpty(NO_BUCKET)
                .flatMap(latest -> latest == NO_BUCKET
                        ? createNextBucket(conversationId, 0, messageEntity)
                        : pushInto(conversationId, latest, messageEntity)
                                .flatMap(appended -> Boolean.TRUE.equals(appended)
                                        ? Mono.<Void>empty()
                                        : createNextBucket(conversationId, latest + 1, messageEntity)))
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
                .set("updatedAt", Instant.now());

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
        return conversationRepository.findListedByParticipant(userId)
                .map(conversationMapper::toConversationSummary);
    }

    @Override
    public Mono<ConversationDetail> findByEventId(String eventId, String requesterId) {
        return conversationRepository.findByEventIdAndParticipantIdsContaining(eventId, requesterId)
                .flatMap(this::loadConversationWithMessages);
    }

    /**
     * Updates the group when the stored list is older; otherwise inserts it. The insert failing on
     * the unique event id means the group exists with this version or a newer one — a replay or a
     * record overtaken by a later one — and there is nothing to do.
     *
     * <p>No first bucket is written: the first message creates it (see {@link #appendToLatestBucket}),
     * and most groups never get one.
     */
    @Override
    public Mono<Void> saveEventGroup(EventGroup group) {
        return conversationRepository
                .updateEventGroup(group.eventId(), group.version(), group.memberIds(), group.title(), group.organizerId())
                .filter(updated -> updated == 0)
                .flatMap(none -> conversationRepository.insert(new ConversationEntity(
                        null, group.memberIds(), null, Instant.now(),
                        ConversationType.EVENT.name(), group.eventId(), group.title(), group.version(),
                        group.organizerId())))
                .onErrorResume(DuplicateKeyException.class, stale -> Mono.empty())
                .then();
    }

    /**
     * A group never created here still gets its empty conversation, holding the version, so that a
     * late change for the deleted event is recognised as older and ignored.
     */
    @Override
    public Mono<String> closeEventGroup(String eventId, long version) {
        return conversationRepository.closeEventGroup(eventId, version)
                .flatMap(updated -> updated > 0
                        ? conversationRepository.findByEventId(eventId)
                                .flatMap(conversation -> messageBucketRepository.deleteByConversationId(conversation.getId())
                                        .thenReturn(conversation.getId()))
                        : conversationRepository.insert(new ConversationEntity(null, List.of(), null, Instant.now(),
                                        ConversationType.EVENT.name(), eventId, null, version, null))
                                .onErrorResume(DuplicateKeyException.class, newer -> Mono.empty())
                                .then(Mono.empty()));
    }

    /** In the bucket that holds the message, and in the conversation's preview if it is the last one. */
    @Override
    public Mono<Void> markPhotoRemoved(String conversationId, String messageId) {
        return messageBucketRepository.markPhotoRemoved(conversationId, messageId)
                .then(conversationRepository.markLastPhotoRemoved(conversationId, messageId))
                .then();
    }

    /** Read from its bucket alone, projected to the one message; the mapper leaves the conversation id to the caller. */
    @Override
    public Mono<Message> findMessage(String conversationId, String messageId) {
        return messageBucketRepository.findWithMessage(conversationId, messageId)
                .flatMapIterable(MessageBucketEntity::getMessages)
                .filter(message -> messageId.equals(message.messageId()))
                .next()
                .map(conversationMapper::toMessage);
    }
}
