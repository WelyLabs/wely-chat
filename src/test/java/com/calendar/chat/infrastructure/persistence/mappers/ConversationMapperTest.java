package com.calendar.chat.infrastructure.persistence.mappers;

import com.calendar.chat.infrastructure.persistence.models.dtos.PhotoEntity;
import com.calendar.chat.domain.models.Photo;
import com.calendar.chat.domain.models.ConversationDetail;
import com.calendar.chat.domain.models.ConversationSummary;
import com.calendar.chat.domain.models.ConversationType;
import com.calendar.chat.domain.models.Message;
import com.calendar.chat.domain.models.MessageBucket;
import com.calendar.chat.domain.models.MessageType;
import com.calendar.chat.infrastructure.persistence.models.dtos.MessageEntity;
import com.calendar.chat.infrastructure.persistence.models.entities.ConversationEntity;
import com.calendar.chat.infrastructure.persistence.models.entities.MessageBucketEntity;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers every method of {@link ConversationMapper}, against the generated implementation.
 *
 * <p>Written because nothing else exercises it: {@code MongoChatRepositoryAdapterTest} mocks the
 * mapper, so the generated class was never executed and every one of its lines counted as
 * uncovered. The field names differ on both sides of this boundary — the entities abbreviate
 * them for MongoDB, which stores every key in every document — so a mapping that silently drops
 * a field is exactly the mistake worth pinning down.
 */
class ConversationMapperTest {

    private final ConversationMapper mapper = new ConversationMapperImpl();

    private static final Instant SENT_AT = Instant.parse("2025-06-01T12:30:00Z");

    private static Message message() {
        return new Message("m-1", "alice", "Alice", "bob", "conv-1", "hello", SENT_AT);
    }

    private static MessageEntity messageEntity() {
        return new MessageEntity("m-1", "alice", "Alice", "hello", SENT_AT, null, null);
    }

    @Test
    @DisplayName("the message id survives the round trip")
    void messageId_shouldSurviveTheRoundTrip() {
        // The bug this pins: Message.id and MessageEntity.messageId differ in name, MapStruct
        // maps by name, so the id was silently dropped in both directions. Every stored message
        // had a null m_id and every message read back had no id — nothing failed, the id simply
        // was not there. The mapper is now declared with unmappedTargetPolicy = ERROR so the
        // next field whose names differ stops the build instead.
        Message roundTripped = mapper.toMessage(mapper.toMessageEntity(message()));

        assertThat(roundTripped.id()).isEqualTo("m-1");
    }

    @Test
    @DisplayName("a domain message becomes an entity, abbreviated field names and all")
    void toMessageEntity_shouldCarryEveryStoredField() {
        MessageEntity entity = mapper.toMessageEntity(message());

        assertThat(entity.messageId()).isEqualTo("m-1");
        assertThat(entity.senderId()).isEqualTo("alice");
        assertThat(entity.senderName()).isEqualTo("Alice");
        assertThat(entity.content()).isEqualTo("hello");
        assertThat(entity.timestamp()).isEqualTo(SENT_AT);
    }

    @Test
    @DisplayName("an entity becomes a domain message")
    void toMessage_shouldCarryEveryStoredField() {
        Message domain = mapper.toMessage(messageEntity());

        assertThat(domain.id()).isEqualTo("m-1");
        assertThat(domain.senderId()).isEqualTo("alice");
        assertThat(domain.senderName()).isEqualTo("Alice");
        assertThat(domain.content()).isEqualTo("hello");
        assertThat(domain.timestamp()).isEqualTo(SENT_AT);
    }

    @Test
    @DisplayName("the receiver and conversation are not stored on a message")
    void toMessage_shouldLeaveTheFieldsTheBucketAlreadyCarries() {
        // A message lives inside a bucket that is already keyed by conversation, and the
        // recipient is a participant of that conversation. Storing either on every message
        // would repeat them fifty times per document.
        Message domain = mapper.toMessage(messageEntity());

        assertThat(domain.receiverId()).isNull();
        assertThat(domain.conversationId()).isNull();
    }

    @Test
    @DisplayName("a summary becomes a conversation entity")
    void toConversationEntity_shouldMapTheSummary() {
        ConversationSummary summary = new ConversationSummary(
                "conv-1", List.of("alice", "bob"), ConversationType.DIRECT, "Bob", SENT_AT, message(), null, true);

        ConversationEntity entity = mapper.toConversationEntity(summary);
        assertThat(entity.getReadOnly()).isTrue();

        assertThat(entity.getId()).isEqualTo("conv-1");
        assertThat(entity.getUpdatedAt()).isEqualTo(SENT_AT);
        assertThat(entity.getLastMessage()).isNotNull();
        assertThat(entity.getLastMessage().content()).isEqualTo("hello");
    }

    @Test
    @DisplayName("an entity becomes a summary, carrying the preview message")
    void toConversationSummary_shouldMapTheEntity() {
        // Only an all-args constructor: the entity declares no no-arg one.
        ConversationEntity entity = ConversationEntity.direct(
                "conv-1", List.of("alice", "bob"), messageEntity(), SENT_AT);

        ConversationSummary summary = mapper.toConversationSummary(entity);

        assertThat(summary.id()).isEqualTo("conv-1");
        // The client names a private conversation after the other participant.
        assertThat(summary.participantIds()).containsExactly("alice", "bob");
        assertThat(summary.updatedAt()).isEqualTo(SENT_AT);
        assertThat(summary.lastMessage().content()).isEqualTo("hello");
    }

    @Test
    @DisplayName("an event group's summary carries its type, title and event")
    void toConversationSummary_shouldMapAnEventGroup() {
        ConversationEntity entity = new ConversationEntity("conv-2", List.of("alice", "bob"), null, SENT_AT,
                "EVENT", "event-1", "Board games", 2L, "alice", null);

        ConversationSummary summary = mapper.toConversationSummary(entity);

        assertThat(summary.type()).isEqualTo(ConversationType.EVENT);
        assertThat(summary.title()).isEqualTo("Board games");
        assertThat(summary.eventId()).isEqualTo("event-1");
    }

    @Test
    @DisplayName("an event group's detail carries its title and event, for the chat header")
    void toConversationDetail_shouldMapAnEventGroup() {
        ConversationEntity entity = new ConversationEntity("conv-2", List.of("alice", "bob"), null, SENT_AT,
                "EVENT", "event-1", "Board games", 2L, "alice", null);

        ConversationDetail detail = mapper.toConversationDetail(entity);

        assertThat(detail.getType()).isEqualTo(ConversationType.EVENT);
        assertThat(detail.getTitle()).isEqualTo("Board games");
        assertThat(detail.getEventId()).isEqualTo("event-1");
    }

    @Test
    void toConversationSummary_shouldMapReadOnly() {
        ConversationEntity entity = ConversationEntity.direct("conv-1", List.of("alice", "bob"), null, SENT_AT);
        entity.setReadOnly(true);

        assertThat(mapper.toConversationSummary(entity).readOnly()).isTrue();
    }

    @Test
    @DisplayName("a conversation stored before account deletion existed has no flag, and reads as writable")
    void toConversationSummary_shouldDefaultReadOnlyToFalse_whenAbsent() {
        ConversationEntity entity = ConversationEntity.direct("conv-1", List.of("alice", "bob"), null, SENT_AT);

        assertThat(mapper.toConversationSummary(entity).readOnly()).isFalse();
        assertThat(mapper.toConversationDetail(entity).isReadOnly()).isFalse();
    }

    @Test
    void toConversationDetail_shouldMapReadOnly() {
        ConversationEntity entity = ConversationEntity.direct("conv-1", List.of("alice", "bob"), null, SENT_AT);
        entity.setReadOnly(true);

        assertThat(mapper.toConversationDetail(entity).isReadOnly()).isTrue();
    }

    @Test
    @DisplayName("a conversation stored before groups existed reads as private")
    void toConversationSummary_shouldDefaultToDirect() {
        ConversationEntity entity = ConversationEntity.direct("conv-1", List.of("alice", "bob"), null, SENT_AT);

        assertThat(mapper.toConversationSummary(entity).type()).isEqualTo(ConversationType.DIRECT);
        assertThat(mapper.toConversationDetail(entity).getType()).isEqualTo(ConversationType.DIRECT);
    }

    @Test
    @DisplayName("an entity becomes a detail, carrying the participants")
    void toConversationDetail_shouldMapTheEntity() {
        ConversationEntity entity = ConversationEntity.direct(
                "conv-1", List.of("alice", "bob"), messageEntity(), SENT_AT);

        ConversationDetail detail = mapper.toConversationDetail(entity);

        assertThat(detail.getId()).isEqualTo("conv-1");
        assertThat(detail.getParticipantIds()).containsExactly("alice", "bob");
    }

    @Test
    @DisplayName("a bucket entity becomes a domain bucket with all its messages")
    void toMessageBucket_shouldMapEveryMessage() {
        MessageBucketEntity entity = new MessageBucketEntity();
        entity.setId("bucket-1");
        entity.setConversationId("conv-1");
        entity.setBucketIndex(3);
        entity.setMessages(List.of(messageEntity(),
                new MessageEntity("m-2", "bob", "Bob", "hi back", SENT_AT.plusSeconds(1 * 60), null, null)));

        MessageBucket bucket = mapper.toMessageBucket(entity);

        assertThat(bucket.conversationId()).isEqualTo("conv-1");
        assertThat(bucket.bucketIndex()).isEqualTo(3);
        assertThat(bucket.messages()).hasSize(2);
        assertThat(bucket.messages()).extracting(Message::content)
                .containsExactly("hello", "hi back");
    }

    @Test
    @DisplayName("an empty bucket maps to an empty list, not to null")
    void toMessageBucket_shouldMapAnEmptyBucket() {
        MessageBucketEntity entity = new MessageBucketEntity();
        entity.setConversationId("conv-1");
        entity.setBucketIndex(0);
        entity.setMessages(List.of());

        MessageBucket bucket = mapper.toMessageBucket(entity);

        assertThat(bucket.messages()).isEmpty();
    }

    @Test
    @DisplayName("null in, null out, on every method")
    void everyMethod_shouldMapNullToNull() {
        // The adapter treats an absent document as an empty Mono. A non-null placeholder here
        // would turn "no such conversation" into one with null fields.
        assertThat(mapper.toMessage(null)).isNull();
        assertThat(mapper.toMessageEntity(null)).isNull();
        assertThat(mapper.toConversationEntity(null)).isNull();
        assertThat(mapper.toConversationSummary(null)).isNull();
        assertThat(mapper.toConversationDetail(null)).isNull();
        assertThat(mapper.toMessageBucket(null)).isNull();
    }

    @Test
    @DisplayName("a photo is stored as its key and size, and comes back without a URL")
    void toMessage_shouldCarryThePhoto_withoutStoringItsUrl() {
        Message withPhoto = new Message("m-1", "alice", "Alice", null, "conv-1", "", SENT_AT,
                new Photo("chat/conv-1/m-1.jpg", 1600, 1200, "https://signed", false));

        MessageEntity entity = mapper.toMessageEntity(withPhoto);
        Message read = mapper.toMessage(entity);

        assertThat(entity.photo()).isEqualTo(new PhotoEntity("chat/conv-1/m-1.jpg", 1600, 1200, false));
        assertThat(read.photo()).isEqualTo(Photo.stored("chat/conv-1/m-1.jpg", 1600, 1200));
    }

    @Test
    void toMessage_shouldKeepARemovedPhoto_andATextMessageWithoutOne() {
        assertThat(mapper.toMessage(new MessageEntity("m-1", "a", "A", "", SENT_AT, new PhotoEntity(null, 0, 0, true), null)).photo())
                .isEqualTo(Photo.REMOVED);
        assertThat(mapper.toMessage(messageEntity()).photo()).isNull();
    }

    @Test
    void toConversationDetail_shouldCarryTheOrganizer() {
        ConversationEntity entity = new ConversationEntity("conv-2", List.of("alice", "bob"), null, SENT_AT,
                "EVENT", "event-1", "Board games", 2L, "alice", null);

        assertThat(mapper.toConversationDetail(entity).getOrganizerId()).isEqualTo("alice");
    }

    @Test
    @DisplayName("a text message stores no type, and an untyped entity reads back as TEXT")
    void toMessageEntity_shouldLeaveTheTypeOutForAText() {
        assertThat(mapper.toMessageEntity(message()).type()).isNull();
        assertThat(mapper.toMessage(messageEntity()).type()).isEqualTo(MessageType.TEXT);
    }

    @Test
    void toMessageEntity_shouldRoundTripASystemMessage() {
        Message notice = Message.system("n-1", "Date fixée", SENT_AT);

        MessageEntity entity = mapper.toMessageEntity(notice);

        assertThat(entity.type()).isEqualTo("SYSTEM");
        assertThat(mapper.toMessage(entity).type()).isEqualTo(MessageType.SYSTEM);
    }

    @Test
    void storedType_shouldBeNullForTextAndNull() {
        assertThat(mapper.storedType(null)).isNull();
        assertThat(mapper.storedType(MessageType.TEXT)).isNull();
        assertThat(mapper.storedType(MessageType.SYSTEM)).isEqualTo("SYSTEM");
    }
}
