package com.calendar.chat.infrastructure.persistence.adapters;

import com.calendar.chat.domain.models.ConversationDetail;
import com.calendar.chat.domain.models.ConversationType;
import com.calendar.chat.domain.models.EventGroup;
import com.calendar.chat.domain.models.Message;
import com.calendar.chat.infrastructure.persistence.models.entities.ConversationEntity;
import com.calendar.chat.infrastructure.persistence.models.entities.MessageBucketEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.time.Instant;
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
// The shared test profile turns index creation off, because the other tests have no database to
// create indexes in. Here there is one, and the index is the subject: without it MongoDB accepts
// a second bucket with the same index, and two concurrent writers fork a conversation's history.
// The first run of this class proved exactly that — both failures traced back to this one line.
@SpringBootTest(properties = "spring.data.mongodb.auto-index-creation=true")
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

    /**
     * Empties the collections without dropping them.
     *
     * <p>Dropping takes the indexes with it, and Spring Data creates them once, lazily, when the
     * entity is first resolved — so a dropped collection comes back unindexed for every test
     * after the first. That is what made the two index-dependent cases here fail even with
     * auto-index-creation switched on.
     */
    @BeforeEach
    void clearDatabase() {
        Query all = new Query();
        mongoTemplate.remove(all, MessageBucketEntity.class)
                .then(mongoTemplate.remove(all, ConversationEntity.class))
                .then()
                .block();
    }

    private ConversationDetail aConversation() {
        return adapter.saveWithInitialBucket(List.of(ALICE, BOB)).block();
    }

    /** Posts and drops the returned members: most cases here are about what gets stored. */
    private reactor.core.publisher.Mono<Void> post(Message message) {
        return adapter.postMessage(message).then();
    }

    private static Message messageFrom(String senderId, String conversationId, String content) {
        return new Message(null, senderId, senderId, BOB, conversationId, content, Instant.now());
    }

    private long bucketCount(String conversationId) {
        return mongoTemplate.find(
                        Query.query(Criteria.where("conversationId").is(conversationId)),
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
                                .concatMap(i -> post(
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
                                .concatMap(i -> post(
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
                                .flatMap(i -> post(
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
                conversation.getId(), "hello", Instant.now());

        StepVerifier.create(post(message)).verifyComplete();

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

        StepVerifier.create(post(
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
        post(messageFrom(ALICE, conversation.getId(), "hi")).block();

        StepVerifier.create(adapter.findUserConversations(ALICE)).expectNextCount(1).verifyComplete();
        StepVerifier.create(adapter.findUserConversations(MALLORY)).verifyComplete();
    }

    @Test
    @DisplayName("the conversation preview follows the last message written")
    void postMessage_shouldUpdateTheConversationPreview() {
        ConversationDetail conversation = aConversation();

        post(messageFrom(ALICE, conversation.getId(), "first")).block();
        post(messageFrom(ALICE, conversation.getId(), "second")).block();

        StepVerifier.create(adapter.findUserConversations(ALICE))
                .assertNext(summary -> assertThat(summary.lastMessage().content()).isEqualTo("second"))
                .verifyComplete();
    }

    @Test
    @DisplayName("the compound index really forbids two buckets with the same index")
    void compoundIndex_shouldBeUniquePerConversationAndBucket() {
        // What makes the duplicate-key retry in appendToLatestBucket meaningful. Without the
        // index MongoDB accepts the second insert and two writers silently fork the history —
        // which is not a hypothesis: the first run of this class had index creation disabled,
        // and the concurrency test above failed with "returned non unique result".
        ConversationDetail conversation = aConversation();
        MessageBucketEntity duplicate = new MessageBucketEntity(
                null, conversation.getId(), 0, new ArrayList<>());

        StepVerifier.create(mongoTemplate.insert(duplicate))
                .expectError(org.springframework.dao.DuplicateKeyException.class)
                .verify();
    }

    // --- event groups -----------------------------------------------------

    private static final EventGroup GROUP = new EventGroup("event-1", "Board games", List.of(ALICE, BOB, "carol"), 2);

    @Test
    @DisplayName("an event group is not mistaken for the private conversation of two of its members")
    void findByParticipantIds_shouldIgnoreEventGroups() {
        adapter.saveEventGroup(GROUP).block();

        StepVerifier.create(adapter.findByParticipantIds(List.of(ALICE, BOB))).verifyComplete();
    }

    @Test
    @DisplayName("an older or replayed member list never overwrites a newer one")
    void saveEventGroup_shouldKeepTheNewestList() {
        adapter.saveEventGroup(GROUP).block();
        adapter.saveEventGroup(new EventGroup("event-1", "Board games", List.of(ALICE, BOB, "carol", "dave"), 3)).block();
        adapter.saveEventGroup(GROUP).block();

        StepVerifier.create(adapter.findByEventId("event-1", "dave"))
                .assertNext(group -> {
                    assertThat(group.getParticipantIds()).containsExactly(ALICE, BOB, "carol", "dave");
                    assertThat(group.getType()).isEqualTo(ConversationType.EVENT);
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("someone who left the group can no longer read it")
    void saveEventGroup_shouldRemoveAMemberWhoLeft() {
        adapter.saveEventGroup(GROUP).block();
        adapter.saveEventGroup(new EventGroup("event-1", "Board games", List.of(ALICE, BOB), 3)).block();

        StepVerifier.create(adapter.findByEventId("event-1", "carol")).verifyComplete();
    }

    @Test
    @DisplayName("a group gets its first bucket with its first message, and every member as recipient")
    void postMessage_shouldWriteTheFirstMessageOfAGroup() {
        adapter.saveEventGroup(GROUP).block();
        String groupId = adapter.findByEventId("event-1", ALICE).block().getId();

        StepVerifier.create(adapter.postMessage(messageFrom(ALICE, groupId, "who brings the cards?")))
                .expectNext(List.of(ALICE, BOB, "carol"))
                .verifyComplete();

        StepVerifier.create(adapter.findBucketByConversationIdAndBucketIndex(groupId, 0, BOB))
                .assertNext(bucket -> assertThat(bucket.messages()).hasSize(1))
                .verifyComplete();
    }

    @Test
    @DisplayName("an event group is listed once someone has written in it")
    void findUserConversations_shouldListAGroupOnceItHasMessages() {
        adapter.saveEventGroup(GROUP).block();
        assertThat(adapter.findUserConversations(BOB).collectList().block()).isEmpty();

        String groupId = adapter.findByEventId("event-1", ALICE).block().getId();
        post(messageFrom(ALICE, groupId, "hi")).block();

        StepVerifier.create(adapter.findUserConversations(BOB))
                .assertNext(summary -> {
                    assertThat(summary.eventId()).isEqualTo("event-1");
                    assertThat(summary.title()).isEqualTo("Board games");
                })
                .verifyComplete();
    }
}
