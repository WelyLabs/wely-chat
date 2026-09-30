package com.calendar.chat.infrastructure.persistence.adapters;

import com.calendar.chat.domain.models.ConversationDetail;
import com.calendar.chat.domain.models.Message;
import com.calendar.chat.infrastructure.persistence.models.entities.MessageBucketEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs {@link MongoChatRepositoryAdapter} against a real MongoDB.
 *
 * <p>The unit test beside this one mocks {@code ReactiveMongoTemplate}, which means it asserts
 * that a {@code Query} was built — not that MongoDB agrees with what it means. Everything here
 * is a property only the database can settle: whether {@code messages.49} really guards the
 * bucket boundary, whether the compound index really rejects a second bucket with the same
 * index, and whether the participant filter really runs in the query rather than after loading.
 *
 * <p>Opt-in, so that a workstation without a working container runtime still runs the rest of
 * the suite. CI sets {@code CI=true} and therefore runs them; locally, pass
 * {@code -Dintegration.tests=true}.
 *
 * <p>The condition deliberately reads an environment variable rather than asking Testcontainers
 * whether Docker is available. That probe connects to the daemon, and a daemon that is running
 * but wedged — which is exactly the failure this project hit — makes the probe itself hang,
 * taking the build with it. A check that cannot answer quickly is not a useful guard.
 */
@Testcontainers
@SpringBootTest
@ActiveProfiles("test")
@EnabledIf("containersRequested")
class MongoChatRepositoryAdapterIntegrationTest {

    @Container
    @ServiceConnection
    static final MongoDBContainer MONGO = new MongoDBContainer("mongo:6.0");

    static boolean containersRequested() {
        return System.getenv("CI") != null
                || Boolean.getBoolean("integration.tests");
    }

    @Autowired
    private MongoChatRepositoryAdapter adapter;

    @Autowired
    private ReactiveMongoTemplate mongoTemplate;

    private static final String ALICE = "alice";
    private static final String BOB = "bob";
    private static final String MALLORY = "mallory";

    @BeforeEach
    void clearDatabase() {
        mongoTemplate.getCollectionNames()
                .flatMap(name -> mongoTemplate.dropCollection(name))
                .then()
                .block();
    }

    private ConversationDetail aConversation() {
        return adapter.saveWithInitialBucket(List.of(ALICE, BOB)).block();
    }

    private static Message messageFrom(String senderId, String conversationId, String content) {
        return new Message(null, senderId, senderId, BOB, conversationId, content, LocalDateTime.now());
    }

    private long bucketCount(String conversationId) {
        return mongoTemplate.find(
                        org.springframework.data.mongodb.core.query.Query.query(
                                org.springframework.data.mongodb.core.query.Criteria
                                        .where("conversationId").is(conversationId)),
                        MessageBucketEntity.class)
                .count().block();
    }

    @Test
    @DisplayName("a new conversation starts with one empty bucket")
    void saveWithInitialBucket_shouldCreateBucketZero() {
        ConversationDetail conversation = aConversation();

        assertThat(conversation).isNotNull();
        assertThat(conversation.getBucketIndex()).isZero();
        assertThat(bucketCount(conversation.getId())).isEqualTo(1);
    }

    @Test
    @DisplayName("messages accumulate in the same bucket while it has room")
    void postMessage_shouldFillTheCurrentBucket() {
        ConversationDetail conversation = aConversation();

        StepVerifier.create(
                        Flux.range(1, 10)
                                .concatMap(i -> adapter.postMessage(
                                        messageFrom(ALICE, conversation.getId(), "message " + i))))
                .verifyComplete();

        assertThat(bucketCount(conversation.getId())).isEqualTo(1);
        StepVerifier.create(adapter.findBucketByConversationIdAndBucketIndex(conversation.getId(), 0, ALICE))
                .assertNext(bucket -> assertThat(bucket.messages()).hasSize(10))
                .verifyComplete();
    }

    @Test
    @DisplayName("the 51st message opens a second bucket, and the first keeps exactly 50")
    void postMessage_shouldRollOverAtTheBucketBoundary() {
        // This is the property the mocked test cannot reach: whether "messages.49 does not
        // exist" really means "this bucket has room" to MongoDB.
        ConversationDetail conversation = aConversation();

        StepVerifier.create(
                        Flux.range(1, MessageBucketEntity.MAX_MESSAGES + 1)
                                .concatMap(i -> adapter.postMessage(
                                        messageFrom(ALICE, conversation.getId(), "message " + i))))
                .verifyComplete();

        assertThat(bucketCount(conversation.getId())).isEqualTo(2);

        StepVerifier.create(adapter.findBucketByConversationIdAndBucketIndex(conversation.getId(), 0, ALICE))
                .assertNext(bucket -> assertThat(bucket.messages())
                        .hasSize(MessageBucketEntity.MAX_MESSAGES))
                .verifyComplete();

        StepVerifier.create(adapter.findBucketByConversationIdAndBucketIndex(conversation.getId(), 1, ALICE))
                .assertNext(bucket -> {
                    assertThat(bucket.messages()).hasSize(1);
                    assertThat(bucket.messages().getFirst().content()).isEqualTo("message 51");
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("no message is lost when many are written at once")
    void postMessage_shouldLoseNothingUnderConcurrency() {
        // The bucket pattern's whole risk is here. Read-modify-write would drop messages that
        // interleave; the conditional $push plus the unique index on (conversationId,
        // bucketIndex) is what makes concurrent writers safe, and only a real database can
        // demonstrate it.
        ConversationDetail conversation = aConversation();
        int total = 120;

        StepVerifier.create(
                        Flux.range(1, total)
                                .flatMap(i -> adapter.postMessage(
                                        messageFrom(ALICE, conversation.getId(), "message " + i)), 16))
                .verifyComplete();

        long stored = Flux.range(0, 10)
                .concatMap(index ->
                        adapter.findBucketByConversationIdAndBucketIndex(conversation.getId(), index, ALICE))
                .map(bucket -> (long) bucket.messages().size())
                .reduce(0L, Long::sum)
                .block();

        assertThat(stored).isEqualTo(total);
    }

    @Test
    @DisplayName("the message id survives a round trip through the database")
    void postMessage_shouldPersistTheMessageId() {
        // The mapper dropped it silently until ConversationMapper was given explicit @Mapping
        // and unmappedTargetPolicy = ERROR. Pinned here too, at the layer where it is stored.
        ConversationDetail conversation = aConversation();
        Message message = new Message("m-42", ALICE, "Alice", BOB,
                conversation.getId(), "hello", LocalDateTime.now());

        StepVerifier.create(adapter.postMessage(message)).verifyComplete();

        StepVerifier.create(adapter.findBucketByConversationIdAndBucketIndex(conversation.getId(), 0, ALICE))
                .assertNext(bucket -> assertThat(bucket.messages().getFirst().id()).isEqualTo("m-42"))
                .verifyComplete();
    }

    @Test
    @DisplayName("someone who is not a participant cannot post into the conversation")
    void postMessage_shouldRejectANonParticipant() {
        // The IDOR guard, filtered in the query rather than after loading the document. A test
        // against a double proves only that a Criteria was assembled.
        ConversationDetail conversation = aConversation();

        StepVerifier.create(adapter.postMessage(
                        messageFrom(MALLORY, conversation.getId(), "let me in"))).verifyComplete();

        StepVerifier.create(adapter.findBucketByConversationIdAndBucketIndex(conversation.getId(), 0, ALICE))
                .assertNext(bucket -> assertThat(bucket.messages()).isEmpty())
                .verifyComplete();
    }

    @Test
    @DisplayName("someone who is not a participant cannot read the conversation")
    void findById_shouldRejectANonParticipant() {
        ConversationDetail conversation = aConversation();

        StepVerifier.create(adapter.findById(conversation.getId(), MALLORY)).verifyComplete();
        StepVerifier.create(adapter.findById(conversation.getId(), ALICE))
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    @DisplayName("a conversation is listed for its participants, and for no one else")
    void findUserConversations_shouldListOnlyTheCallersConversations() {
        ConversationDetail conversation = aConversation();
        adapter.postMessage(messageFrom(ALICE, conversation.getId(), "hi")).block();

        StepVerifier.create(adapter.findUserConversations(ALICE)).expectNextCount(1).verifyComplete();
        StepVerifier.create(adapter.findUserConversations(MALLORY)).verifyComplete();
    }

    @Test
    @DisplayName("the conversation preview follows the last message written")
    void postMessage_shouldUpdateTheConversationPreview() {
        ConversationDetail conversation = aConversation();

        adapter.postMessage(messageFrom(ALICE, conversation.getId(), "first")).block();
        adapter.postMessage(messageFrom(ALICE, conversation.getId(), "second")).block();

        StepVerifier.create(adapter.findUserConversations(ALICE))
                .assertNext(summary -> assertThat(summary.lastMessage().content()).isEqualTo("second"))
                .verifyComplete();
    }

    @Test
    @DisplayName("the compound index really forbids two buckets with the same index")
    void compoundIndex_shouldBeUniquePerConversationAndBucket() {
        // What makes the duplicate-key retry in appendToLatestBucket meaningful. Without the
        // index MongoDB accepts the second insert and two writers silently fork the history.
        ConversationDetail conversation = aConversation();
        MessageBucketEntity duplicate = new MessageBucketEntity(
                null, conversation.getId(), 0, new java.util.ArrayList<>());

        StepVerifier.create(mongoTemplate.insert(duplicate))
                .expectError(org.springframework.dao.DuplicateKeyException.class)
                .verify();
    }
}
