package com.calendar.chat.infrastructure.persistence.adapters;

import com.calendar.chat.domain.models.ConversationSummary;
import com.calendar.chat.domain.models.EventGroup;
import com.calendar.chat.domain.models.Message;
import com.calendar.chat.infrastructure.persistence.mappers.ConversationMapper;
import com.calendar.chat.infrastructure.persistence.models.dtos.MessageEntity;
import com.calendar.chat.infrastructure.persistence.models.entities.ConversationEntity;
import com.calendar.chat.infrastructure.persistence.models.entities.MessageBucketEntity;
import com.calendar.chat.infrastructure.persistence.repositories.ConversationRepository;
import com.calendar.chat.infrastructure.persistence.repositories.MessageBucketRepository;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;

import static com.calendar.chat.infrastructure.persistence.models.entities.MessageBucketEntity.MAX_MESSAGES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MongoChatRepositoryAdapterTest {

    private static final String CONVERSATION_ID = "conv-1";
    private static final String SENDER_ID = "sender-1";

    @Mock private ConversationRepository conversationRepository;
    @Mock private MessageBucketRepository messageBucketRepository;
    @Mock private ConversationMapper conversationMapper;
    @Mock private ReactiveMongoTemplate mongoTemplate;

    private MongoChatRepositoryAdapter adapter;

    private final Message message = new Message(
            null, SENDER_ID, "Sender", "receiver-1", CONVERSATION_ID, "hello", Instant.now());
    private final MessageEntity messageEntity = new MessageEntity(
            "m-1", SENDER_ID, "Sender", "hello", Instant.now(), null, null);

    @BeforeEach
    void setUp() {
        adapter = new MongoChatRepositoryAdapter(
                conversationRepository, messageBucketRepository, conversationMapper, mongoTemplate);
    }

    private static UpdateResult modified(long count) {
        return UpdateResult.acknowledged(count, count, null);
    }

    private static MessageBucketEntity bucket(int index) {
        return new MessageBucketEntity("b-" + index, CONVERSATION_ID, index, List.of());
    }

    // --- access control ---------------------------------------------------

    @Test
    @DisplayName("postMessage writes nothing when the sender is not in the conversation")
    void postMessage_shouldWriteNothingForNonParticipant() {
        when(conversationMapper.toMessageEntity(message)).thenReturn(messageEntity);
        when(conversationRepository.findByIdAndParticipantIdsContaining(CONVERSATION_ID, SENDER_ID))
                .thenReturn(Mono.empty());

        StepVerifier.create(adapter.postMessage(message)).verifyComplete();

        verify(mongoTemplate, never()).updateFirst(any(), any(Update.class), any(Class.class));
        verify(mongoTemplate, never()).insert(any(MessageBucketEntity.class));
    }

    @Test
    @DisplayName("findById filters on participation in the query, not after loading")
    void findById_shouldQueryScopedByParticipant() {
        when(conversationRepository.findByIdAndParticipantIdsContaining(CONVERSATION_ID, "intruder"))
                .thenReturn(Mono.empty());

        StepVerifier.create(adapter.findById(CONVERSATION_ID, "intruder")).verifyComplete();

        verify(conversationRepository).findByIdAndParticipantIdsContaining(CONVERSATION_ID, "intruder");
        verify(messageBucketRepository, never()).findFirstByConversationIdOrderByBucketIndexDesc(any());
    }

    @Test
    @DisplayName("reading a bucket requires participation before the bucket is touched")
    void findBucket_shouldRequireParticipationFirst() {
        when(conversationRepository.findByIdAndParticipantIdsContaining(CONVERSATION_ID, "intruder"))
                .thenReturn(Mono.empty());

        StepVerifier.create(adapter.findBucketByConversationIdAndBucketIndex(CONVERSATION_ID, 0, "intruder"))
                .verifyComplete();

        verify(messageBucketRepository, never()).findByConversationIdAndBucketIndex(any(), any());
    }

    // --- atomic append ----------------------------------------------------

    @Test
    @DisplayName("a bucket with room gets a $push, with no document rewrite")
    void postMessage_shouldPushIntoBucketWithRoom() {
        givenParticipantAndLatestBucket(0);
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(MessageBucketEntity.class)))
                .thenReturn(Mono.just(modified(1)));
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(ConversationEntity.class)))
                .thenReturn(Mono.just(modified(1)));

        StepVerifier.create(adapter.postMessage(message)).expectNextCount(1).verifyComplete();

        ArgumentCaptor<Update> update = ArgumentCaptor.forClass(Update.class);
        verify(mongoTemplate).updateFirst(any(Query.class), update.capture(), eq(MessageBucketEntity.class));
        // Inspected by key: toJson() would need a Mongo codec, absent in a unit test.
        Document pushed = update.getValue().getUpdateObject();
        assertThat(pushed).containsKey("$push");
        assertThat((Document) pushed.get("$push")).containsKey("messages");

        verify(mongoTemplate, never()).insert(any(MessageBucketEntity.class));
        verify(messageBucketRepository, never()).save(any());
    }

    @Test
    @DisplayName("the $push is guarded by the room left in the bucket")
    void postMessage_shouldGuardPushOnRemainingRoom() {
        givenParticipantAndLatestBucket(0);
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(MessageBucketEntity.class)))
                .thenReturn(Mono.just(modified(1)));
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(ConversationEntity.class)))
                .thenReturn(Mono.just(modified(1)));

        StepVerifier.create(adapter.postMessage(message)).expectNextCount(1).verifyComplete();

        ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
        verify(mongoTemplate).updateFirst(query.capture(), any(Update.class), eq(MessageBucketEntity.class));
        String json = query.getValue().getQueryObject().toJson();
        assertThat(json).contains("messages." + (MAX_MESSAGES - 1));
        assertThat(json).contains("$exists");
    }

    @Test
    @DisplayName("a full bucket triggers creation of the next one")
    void postMessage_shouldRollOverWhenBucketFull() {
        givenParticipantAndLatestBucket(3);
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(MessageBucketEntity.class)))
                .thenReturn(Mono.just(modified(0)));   // no document had room
        when(mongoTemplate.insert(any(MessageBucketEntity.class)))
                .thenReturn(Mono.just(bucket(4)));
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(ConversationEntity.class)))
                .thenReturn(Mono.just(modified(1)));

        StepVerifier.create(adapter.postMessage(message)).expectNextCount(1).verifyComplete();

        ArgumentCaptor<MessageBucketEntity> inserted = ArgumentCaptor.forClass(MessageBucketEntity.class);
        verify(mongoTemplate).insert(inserted.capture());
        assertThat(inserted.getValue().getBucketIndex()).isEqualTo(4);
        assertThat(inserted.getValue().getMessages()).containsExactly(messageEntity);
    }

    @Test
    @DisplayName("a collision on the next bucket is retried against the one the other writer created")
    void postMessage_shouldRetryOnDuplicateKey() {
        when(conversationMapper.toMessageEntity(message)).thenReturn(messageEntity);
        when(conversationRepository.findByIdAndParticipantIdsContaining(CONVERSATION_ID, SENDER_ID))
                .thenReturn(Mono.just(ConversationEntity.direct(
                        CONVERSATION_ID, List.of(SENDER_ID, "receiver-1"), null, Instant.now())));

        // first pass: bucket 3 is full; second: the winner's bucket 4 has room
        when(messageBucketRepository.findFirstByConversationIdOrderByBucketIndexDesc(CONVERSATION_ID))
                .thenReturn(Mono.just(bucket(3)), Mono.just(bucket(4)));
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(MessageBucketEntity.class)))
                .thenReturn(Mono.just(modified(0)), Mono.just(modified(1)));
        when(mongoTemplate.insert(any(MessageBucketEntity.class)))
                .thenReturn(Mono.error(new DuplicateKeyException("conversation_bucket_unique")));
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(ConversationEntity.class)))
                .thenReturn(Mono.just(modified(1)));

        StepVerifier.create(adapter.postMessage(message)).expectNextCount(1).verifyComplete();

        verify(messageBucketRepository, times(2))
                .findFirstByConversationIdOrderByBucketIndexDesc(CONVERSATION_ID);
        verify(mongoTemplate, times(2))
                .updateFirst(any(Query.class), any(Update.class), eq(MessageBucketEntity.class));
    }

    @Test
    @DisplayName("the conversation preview is updated with $set, never by rewriting the document")
    void postMessage_shouldTouchConversationWithTargetedSet() {
        givenParticipantAndLatestBucket(0);
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(MessageBucketEntity.class)))
                .thenReturn(Mono.just(modified(1)));
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(ConversationEntity.class)))
                .thenReturn(Mono.just(modified(1)));

        StepVerifier.create(adapter.postMessage(message)).expectNextCount(1).verifyComplete();

        ArgumentCaptor<Update> update = ArgumentCaptor.forClass(Update.class);
        verify(mongoTemplate).updateFirst(any(Query.class), update.capture(), eq(ConversationEntity.class));
        Document set = (Document) update.getValue().getUpdateObject().get("$set");
        assertThat(set).containsKeys("lastMessage", "updatedAt");

        verify(conversationRepository, never()).save(any());
    }

    // --- robustness -------------------------------------------------------

    @Test
    @DisplayName("a conversation with no initial bucket reads as empty instead of vanishing")
    void findById_shouldTolerateMissingInitialBucket() {
        ConversationEntity conversation = ConversationEntity.direct(
                CONVERSATION_ID, List.of(SENDER_ID), null, Instant.now());

        when(conversationRepository.findByIdAndParticipantIdsContaining(CONVERSATION_ID, SENDER_ID))
                .thenReturn(Mono.just(conversation));
        when(messageBucketRepository.findFirstByConversationIdOrderByBucketIndexDesc(CONVERSATION_ID))
                .thenReturn(Mono.empty());
        when(conversationMapper.toConversationDetail(conversation))
                .thenReturn(new com.calendar.chat.domain.models.ConversationDetail());

        StepVerifier.create(adapter.findById(CONVERSATION_ID, SENDER_ID))
                .assertNext(detail -> {
                    assertThat(detail.getMessages()).isEmpty();
                    assertThat(detail.getBucketIndex()).isZero();
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("postMessage returns the stored members: the recipients come from there")
    void postMessage_shouldReturnTheConversationMembers() {
        givenParticipantAndLatestBucket(0);
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(MessageBucketEntity.class)))
                .thenReturn(Mono.just(modified(1)));
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(ConversationEntity.class)))
                .thenReturn(Mono.just(modified(1)));

        StepVerifier.create(adapter.postMessage(message))
                .expectNext(List.of(SENDER_ID, "receiver-1"))
                .verifyComplete();
    }

    @Test
    @DisplayName("the first message of a group without a bucket creates bucket 0 instead of vanishing")
    void postMessage_shouldCreateTheFirstBucket_whenThereIsNone() {
        when(conversationMapper.toMessageEntity(message)).thenReturn(messageEntity);
        when(conversationRepository.findByIdAndParticipantIdsContaining(CONVERSATION_ID, SENDER_ID))
                .thenReturn(Mono.just(ConversationEntity.direct(
                        CONVERSATION_ID, List.of(SENDER_ID), null, Instant.now())));
        when(messageBucketRepository.findFirstByConversationIdOrderByBucketIndexDesc(CONVERSATION_ID))
                .thenReturn(Mono.empty());
        when(mongoTemplate.insert(any(MessageBucketEntity.class))).thenAnswer(call -> Mono.just(call.getArgument(0)));
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(ConversationEntity.class)))
                .thenReturn(Mono.just(modified(1)));

        StepVerifier.create(adapter.postMessage(message)).expectNextCount(1).verifyComplete();

        ArgumentCaptor<MessageBucketEntity> created = ArgumentCaptor.forClass(MessageBucketEntity.class);
        verify(mongoTemplate).insert(created.capture());
        assertThat(created.getValue().getBucketIndex()).isZero();
        assertThat(created.getValue().getMessages()).containsExactly(messageEntity);
    }

    // --- event groups -----------------------------------------------------

    @Test
    void findUserConversations_shouldListWhatTheRepositoryKeeps() {
        ConversationEntity entity = ConversationEntity.direct(CONVERSATION_ID, List.of("me"), null, null);
        ConversationSummary summary = new ConversationSummary(CONVERSATION_ID, List.of("me"), null, null, null, null, null);
        when(conversationRepository.findListedByParticipant("me")).thenReturn(Flux.just(entity));
        when(conversationMapper.toConversationSummary(entity)).thenReturn(summary);

        StepVerifier.create(adapter.findUserConversations("me")).expectNext(summary).verifyComplete();
    }

    @Test
    @DisplayName("an event's group is looked up scoped to its members")
    void findByEventId_shouldQueryScopedByMember() {
        when(conversationRepository.findByEventIdAndParticipantIdsContaining("event-1", "intruder"))
                .thenReturn(Mono.empty());

        StepVerifier.create(adapter.findByEventId("event-1", "intruder")).verifyComplete();

        verify(messageBucketRepository, never()).findFirstByConversationIdOrderByBucketIndexDesc(any());
    }

    private static final EventGroup GROUP = new EventGroup("event-1", "Board games", List.of("ines", "me"), 3);

    @Test
    @DisplayName("an existing older group is updated in place, nothing inserted")
    void saveEventGroup_shouldUpdateAnOlderGroup() {
        when(conversationRepository.updateEventGroup("event-1", 3, GROUP.memberIds(), "Board games", null))
                .thenReturn(Mono.just(1L));

        StepVerifier.create(adapter.saveEventGroup(GROUP)).verifyComplete();

        verify(conversationRepository, never()).insert(any(ConversationEntity.class));
    }

    @Test
    @DisplayName("a group seen for the first time is inserted as an EVENT conversation")
    void saveEventGroup_shouldInsertANewGroup() {
        when(conversationRepository.updateEventGroup("event-1", 3, GROUP.memberIds(), "Board games", null))
                .thenReturn(Mono.just(0L));
        when(conversationRepository.insert(any(ConversationEntity.class)))
                .thenAnswer(call -> Mono.just(call.getArgument(0)));

        StepVerifier.create(adapter.saveEventGroup(GROUP)).verifyComplete();

        ArgumentCaptor<ConversationEntity> inserted = ArgumentCaptor.forClass(ConversationEntity.class);
        verify(conversationRepository).insert(inserted.capture());
        assertThat(inserted.getValue().getType()).isEqualTo("EVENT");
        assertThat(inserted.getValue().getEventId()).isEqualTo("event-1");
        assertThat(inserted.getValue().getTitle()).isEqualTo("Board games");
        assertThat(inserted.getValue().getParticipantIds()).containsExactly("ines", "me");
        assertThat(inserted.getValue().getGroupVersion()).isEqualTo(3L);
    }

    @Test
    @DisplayName("a replayed or overtaken list is ignored: the unique event id refuses the insert")
    void saveEventGroup_shouldIgnoreAStaleList() {
        when(conversationRepository.updateEventGroup("event-1", 3, GROUP.memberIds(), "Board games", null))
                .thenReturn(Mono.just(0L));
        when(conversationRepository.insert(any(ConversationEntity.class)))
                .thenReturn(Mono.error(new DuplicateKeyException("eventId")));

        StepVerifier.create(adapter.saveEventGroup(GROUP)).verifyComplete();
    }

    @Test
    void saveEventGroup_shouldFail_whenMongoFails() {
        when(conversationRepository.updateEventGroup("event-1", 3, GROUP.memberIds(), "Board games", null))
                .thenReturn(Mono.error(new IllegalStateException("mongo down")));

        StepVerifier.create(adapter.saveEventGroup(GROUP)).verifyError(IllegalStateException.class);
    }

    private void givenParticipantAndLatestBucket(int latestIndex) {
        when(conversationMapper.toMessageEntity(message)).thenReturn(messageEntity);
        when(conversationRepository.findByIdAndParticipantIdsContaining(CONVERSATION_ID, SENDER_ID))
                .thenReturn(Mono.just(ConversationEntity.direct(
                        CONVERSATION_ID, List.of(SENDER_ID, "receiver-1"), null, Instant.now())));
        when(messageBucketRepository.findFirstByConversationIdOrderByBucketIndexDesc(CONVERSATION_ID))
                .thenReturn(Mono.just(bucket(latestIndex)));
    }

    @Test
    @DisplayName("removing a photo marks it in its bucket and in the conversation preview")
    void markPhotoRemoved_shouldUpdateTheBucketAndThePreview() {
        when(messageBucketRepository.markPhotoRemoved("conv-1", "m-1")).thenReturn(Mono.just(1L));
        when(conversationRepository.markLastPhotoRemoved("conv-1", "m-1")).thenReturn(Mono.just(0L));

        StepVerifier.create(adapter.markPhotoRemoved("conv-1", "m-1")).verifyComplete();

        verify(messageBucketRepository).markPhotoRemoved("conv-1", "m-1");
        verify(conversationRepository).markLastPhotoRemoved("conv-1", "m-1");
    }

    @Test
    void findMessage_shouldReadTheOneMessageFromItsBucket() {
        MessageEntity entity = new MessageEntity("m-1", "alice", "Alice", "Podium", Instant.EPOCH, null, null);
        Message message = new Message("m-1", "alice", "Alice", null, null, "Podium", Instant.EPOCH);
        when(messageBucketRepository.findWithMessage("conv-1", "m-1"))
                .thenReturn(Mono.just(new MessageBucketEntity("b-1", "conv-1", 3, List.of(entity))));
        when(conversationMapper.toMessage(entity)).thenReturn(message);

        StepVerifier.create(adapter.findMessage("conv-1", "m-1")).expectNext(message).verifyComplete();
    }

    @Test
    void findMessage_shouldBeEmpty_whenNoBucketHoldsTheMessage() {
        when(messageBucketRepository.findWithMessage("conv-1", "gone")).thenReturn(Mono.empty());

        StepVerifier.create(adapter.findMessage("conv-1", "gone")).verifyComplete();
    }

    @Test
    @DisplayName("only the requested message is returned, even if the projection were to hand back more")
    void findMessage_shouldKeepOnlyTheRequestedMessage() {
        MessageEntity other = new MessageEntity("m-0", "bob", "Bob", "hi", Instant.EPOCH, null, null);
        MessageEntity wanted = new MessageEntity("m-1", "alice", "Alice", "Podium", Instant.EPOCH, null, null);
        Message message = new Message("m-1", "alice", "Alice", null, null, "Podium", Instant.EPOCH);
        when(messageBucketRepository.findWithMessage("conv-1", "m-1"))
                .thenReturn(Mono.just(new MessageBucketEntity("b-1", "conv-1", 3, List.of(other, wanted))));
        when(conversationMapper.toMessage(wanted)).thenReturn(message);

        StepVerifier.create(adapter.findMessage("conv-1", "m-1")).expectNext(message).verifyComplete();
    }

    // --- system messages --------------------------------------------------

    private static final Message NOTICE = Message.system("n-1", "Date fixée", Instant.EPOCH);

    private static ConversationEntity eventGroup(List<String> memberIds) {
        return new ConversationEntity(CONVERSATION_ID, memberIds, null, Instant.EPOCH, "EVENT", "event-1",
                "Board games", 2L, "olivia");
    }

    @Test
    @DisplayName("a notice is written into the event's group with no sender check, and every member returned")
    void postSystemMessage_shouldAppendWithoutParticipantCheck() {
        when(conversationRepository.findByEventId("event-1")).thenReturn(Mono.just(eventGroup(List.of("olivia", "ines"))));
        when(messageBucketRepository.existsWithMessage(CONVERSATION_ID, "n-1")).thenReturn(Mono.just(false));
        when(conversationMapper.toMessageEntity(any())).thenReturn(messageEntity);
        when(messageBucketRepository.findFirstByConversationIdOrderByBucketIndexDesc(CONVERSATION_ID))
                .thenReturn(Mono.just(bucket(0)));
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(MessageBucketEntity.class)))
                .thenReturn(Mono.just(modified(1)));
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(ConversationEntity.class)))
                .thenReturn(Mono.just(modified(1)));

        StepVerifier.create(adapter.postSystemMessage("event-1", NOTICE))
                .assertNext(posted -> {
                    assertThat(posted.duplicate()).isFalse();
                    assertThat(posted.participantIds()).containsExactly("olivia", "ines");
                    assertThat(posted.message().conversationId()).isEqualTo(CONVERSATION_ID);
                    assertThat(posted.message().id()).isEqualTo("n-1");
                })
                .verifyComplete();

        verify(conversationRepository, never()).findByIdAndParticipantIdsContaining(any(), any());
        verify(conversationMapper).toMessageEntity(argThat(stored -> CONVERSATION_ID.equals(stored.conversationId())));
        verify(mongoTemplate).updateFirst(any(Query.class), any(Update.class), eq(ConversationEntity.class));
    }

    @Test
    @DisplayName("a notice already in the conversation is not written twice")
    void postSystemMessage_shouldWriteNothing_whenTheMessageIsAlreadyStored() {
        when(conversationRepository.findByEventId("event-1")).thenReturn(Mono.just(eventGroup(List.of("olivia"))));
        when(messageBucketRepository.existsWithMessage(CONVERSATION_ID, "n-1")).thenReturn(Mono.just(true));

        StepVerifier.create(adapter.postSystemMessage("event-1", NOTICE))
                .assertNext(posted -> assertThat(posted.duplicate()).isTrue())
                .verifyComplete();

        verify(mongoTemplate, never()).updateFirst(any(), any(Update.class), any(Class.class));
        verify(mongoTemplate, never()).insert(any(MessageBucketEntity.class));
    }

    @Test
    void postSystemMessage_shouldBeEmpty_whenTheEventHasNoGroup() {
        when(conversationRepository.findByEventId("event-1")).thenReturn(Mono.empty());

        StepVerifier.create(adapter.postSystemMessage("event-1", NOTICE)).verifyComplete();

        verify(messageBucketRepository, never()).existsWithMessage(any(), any());
        verify(mongoTemplate, never()).updateFirst(any(), any(Update.class), any(Class.class));
    }

    @Test
    void postSystemMessage_shouldReturnNoRecipients_whenTheGroupHasNoMemberList() {
        when(conversationRepository.findByEventId("event-1")).thenReturn(Mono.just(eventGroup(null)));
        when(messageBucketRepository.existsWithMessage(CONVERSATION_ID, "n-1")).thenReturn(Mono.just(true));

        StepVerifier.create(adapter.postSystemMessage("event-1", NOTICE))
                .assertNext(posted -> assertThat(posted.participantIds()).isEmpty())
                .verifyComplete();
    }
}
