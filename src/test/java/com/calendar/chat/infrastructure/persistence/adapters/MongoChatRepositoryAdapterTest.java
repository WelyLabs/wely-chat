package com.calendar.chat.infrastructure.persistence.adapters;

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
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.LocalDateTime;
import java.util.List;

import static com.calendar.chat.infrastructure.persistence.models.entities.MessageBucketEntity.MAX_MESSAGES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
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
            null, SENDER_ID, "Sender", "receiver-1", CONVERSATION_ID, "hello", LocalDateTime.now());
    private final MessageEntity messageEntity = new MessageEntity(
            "m-1", SENDER_ID, "Sender", "hello", LocalDateTime.now());

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

    // --- contrôle d'accès -------------------------------------------------

    @Test
    @DisplayName("postMessage n'écrit rien si l'expéditeur ne participe pas à la conversation")
    void postMessage_shouldWriteNothingForNonParticipant() {
        when(conversationMapper.toMessageEntity(message)).thenReturn(messageEntity);
        when(conversationRepository.findByIdAndParticipantIdsContaining(CONVERSATION_ID, SENDER_ID))
                .thenReturn(Mono.empty());

        StepVerifier.create(adapter.postMessage(message)).verifyComplete();

        verify(mongoTemplate, never()).updateFirst(any(), any(Update.class), any(Class.class));
        verify(mongoTemplate, never()).insert(any(MessageBucketEntity.class));
    }

    @Test
    @DisplayName("findById filtre sur la participation, pas après chargement")
    void findById_shouldQueryScopedByParticipant() {
        when(conversationRepository.findByIdAndParticipantIdsContaining(CONVERSATION_ID, "intruder"))
                .thenReturn(Mono.empty());

        StepVerifier.create(adapter.findById(CONVERSATION_ID, "intruder")).verifyComplete();

        verify(conversationRepository).findByIdAndParticipantIdsContaining(CONVERSATION_ID, "intruder");
        verify(messageBucketRepository, never()).findFirstByConversationIdOrderByBucketIndexDesc(any());
    }

    @Test
    @DisplayName("la lecture d'un bucket exige la participation avant de toucher au bucket")
    void findBucket_shouldRequireParticipationFirst() {
        when(conversationRepository.findByIdAndParticipantIdsContaining(CONVERSATION_ID, "intruder"))
                .thenReturn(Mono.empty());

        StepVerifier.create(adapter.findBucketByConversationIdAndBucketIndex(CONVERSATION_ID, 0, "intruder"))
                .verifyComplete();

        verify(messageBucketRepository, never()).findByConversationIdAndBucketIndex(any(), any());
    }

    // --- append atomique -------------------------------------------------

    @Test
    @DisplayName("un bucket avec de la place reçoit un $push, sans réécriture du document")
    void postMessage_shouldPushIntoBucketWithRoom() {
        givenParticipantAndLatestBucket(0);
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(MessageBucketEntity.class)))
                .thenReturn(Mono.just(modified(1)));
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(ConversationEntity.class)))
                .thenReturn(Mono.just(modified(1)));

        StepVerifier.create(adapter.postMessage(message)).verifyComplete();

        ArgumentCaptor<Update> update = ArgumentCaptor.forClass(Update.class);
        verify(mongoTemplate).updateFirst(any(Query.class), update.capture(), eq(MessageBucketEntity.class));
        // Inspection par clés : toJson() exigerait un codec Mongo, absent en test unitaire.
        Document pushed = update.getValue().getUpdateObject();
        assertThat(pushed).containsKey("$push");
        assertThat((Document) pushed.get("$push")).containsKey("messages");

        verify(mongoTemplate, never()).insert(any(MessageBucketEntity.class));
        verify(messageBucketRepository, never()).save(any());
    }

    @Test
    @DisplayName("le $push est conditionné par la place restante dans le bucket")
    void postMessage_shouldGuardPushOnRemainingRoom() {
        givenParticipantAndLatestBucket(0);
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(MessageBucketEntity.class)))
                .thenReturn(Mono.just(modified(1)));
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(ConversationEntity.class)))
                .thenReturn(Mono.just(modified(1)));

        StepVerifier.create(adapter.postMessage(message)).verifyComplete();

        ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
        verify(mongoTemplate).updateFirst(query.capture(), any(Update.class), eq(MessageBucketEntity.class));
        String json = query.getValue().getQueryObject().toJson();
        assertThat(json).contains("messages." + (MAX_MESSAGES - 1));
        assertThat(json).contains("$exists");
    }

    @Test
    @DisplayName("un bucket plein déclenche la création du bucket suivant")
    void postMessage_shouldRollOverWhenBucketFull() {
        givenParticipantAndLatestBucket(3);
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(MessageBucketEntity.class)))
                .thenReturn(Mono.just(modified(0)));   // aucun document n'avait de place
        when(mongoTemplate.insert(any(MessageBucketEntity.class)))
                .thenReturn(Mono.just(bucket(4)));
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(ConversationEntity.class)))
                .thenReturn(Mono.just(modified(1)));

        StepVerifier.create(adapter.postMessage(message)).verifyComplete();

        ArgumentCaptor<MessageBucketEntity> inserted = ArgumentCaptor.forClass(MessageBucketEntity.class);
        verify(mongoTemplate).insert(inserted.capture());
        assertThat(inserted.getValue().getBucketIndex()).isEqualTo(4);
        assertThat(inserted.getValue().getMessages()).containsExactly(messageEntity);
    }

    @Test
    @DisplayName("une collision sur le bucket suivant est retentée contre le bucket créé par l'autre écrivain")
    void postMessage_shouldRetryOnDuplicateKey() {
        when(conversationMapper.toMessageEntity(message)).thenReturn(messageEntity);
        when(conversationRepository.findByIdAndParticipantIdsContaining(CONVERSATION_ID, SENDER_ID))
                .thenReturn(Mono.just(new ConversationEntity(
                        CONVERSATION_ID, List.of(SENDER_ID, "receiver-1"), null, LocalDateTime.now())));

        // 1er passage : bucket 3 plein ; 2e passage : le bucket 4 du concurrent a de la place
        when(messageBucketRepository.findFirstByConversationIdOrderByBucketIndexDesc(CONVERSATION_ID))
                .thenReturn(Mono.just(bucket(3)), Mono.just(bucket(4)));
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(MessageBucketEntity.class)))
                .thenReturn(Mono.just(modified(0)), Mono.just(modified(1)));
        when(mongoTemplate.insert(any(MessageBucketEntity.class)))
                .thenReturn(Mono.error(new DuplicateKeyException("conversation_bucket_unique")));
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(ConversationEntity.class)))
                .thenReturn(Mono.just(modified(1)));

        StepVerifier.create(adapter.postMessage(message)).verifyComplete();

        verify(messageBucketRepository, times(2))
                .findFirstByConversationIdOrderByBucketIndexDesc(CONVERSATION_ID);
        verify(mongoTemplate, times(2))
                .updateFirst(any(Query.class), any(Update.class), eq(MessageBucketEntity.class));
    }

    @Test
    @DisplayName("l'aperçu de la conversation est mis à jour par $set, jamais par réécriture complète")
    void postMessage_shouldTouchConversationWithTargetedSet() {
        givenParticipantAndLatestBucket(0);
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(MessageBucketEntity.class)))
                .thenReturn(Mono.just(modified(1)));
        when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(ConversationEntity.class)))
                .thenReturn(Mono.just(modified(1)));

        StepVerifier.create(adapter.postMessage(message)).verifyComplete();

        ArgumentCaptor<Update> update = ArgumentCaptor.forClass(Update.class);
        verify(mongoTemplate).updateFirst(any(Query.class), update.capture(), eq(ConversationEntity.class));
        Document set = (Document) update.getValue().getUpdateObject().get("$set");
        assertThat(set).containsKeys("lastMessage", "updatedAt");

        verify(conversationRepository, never()).save(any());
    }

    // --- robustesse ------------------------------------------------------

    @Test
    @DisplayName("une conversation sans bucket initial se lit comme vide au lieu de disparaître")
    void findById_shouldTolerateMissingInitialBucket() {
        ConversationEntity conversation = new ConversationEntity(
                CONVERSATION_ID, List.of(SENDER_ID), null, LocalDateTime.now());

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

    private void givenParticipantAndLatestBucket(int latestIndex) {
        when(conversationMapper.toMessageEntity(message)).thenReturn(messageEntity);
        when(conversationRepository.findByIdAndParticipantIdsContaining(CONVERSATION_ID, SENDER_ID))
                .thenReturn(Mono.just(new ConversationEntity(
                        CONVERSATION_ID, List.of(SENDER_ID, "receiver-1"), null, LocalDateTime.now())));
        when(messageBucketRepository.findFirstByConversationIdOrderByBucketIndexDesc(CONVERSATION_ID))
                .thenReturn(Mono.just(bucket(latestIndex)));
    }
}
