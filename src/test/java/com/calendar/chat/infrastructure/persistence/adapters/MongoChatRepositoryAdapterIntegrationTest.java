package com.calendar.chat.infrastructure.persistence.adapters;

import com.calendar.chat.domain.models.Photo;
import com.calendar.chat.domain.models.ConversationDetail;
import com.calendar.chat.domain.models.ConversationType;
import com.calendar.chat.domain.models.EventGroup;
import com.calendar.chat.domain.models.Message;
import com.calendar.chat.domain.models.MessageType;
import com.calendar.chat.infrastructure.persistence.models.entities.ConversationEntity;
import com.calendar.chat.infrastructure.persistence.models.entities.MessageBucketEntity;
import com.calendar.chat.infrastructure.persistence.repositories.ConversationRepository;
import com.calendar.chat.infrastructure.persistence.repositories.MessageBucketRepository;
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

    @Autowired
    private MessageBucketRepository messageBucketRepository;

    @Autowired
    private ConversationRepository conversationRepository;

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
    @DisplayName("a deleted event's group is emptied for good: no member, no message, and a late list is ignored")
    void closeEventGroup_shouldEmptyTheGroupForGood() {
        adapter.saveEventGroup(GROUP).block();
        String groupId = adapter.findByEventId("event-1", ALICE).block().getId();
        adapter.postMessage(messageFrom(ALICE, groupId, "who brings the cards?")).block();

        StepVerifier.create(adapter.closeEventGroup("event-1", 5)).expectNext(groupId).verifyComplete();
        adapter.saveEventGroup(new EventGroup("event-1", "Board games", List.of(ALICE, BOB), 4)).block();

        StepVerifier.create(adapter.findByEventId("event-1", ALICE)).verifyComplete();
        StepVerifier.create(adapter.findUserConversations(ALICE)).verifyComplete();
        StepVerifier.create(adapter.closeEventGroup("event-1", 5)).verifyComplete();
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

    @Test
    @DisplayName("a removed photo loses its key in its bucket and in the preview, and the message keeps its place")
    void markPhotoRemoved_shouldMarkTheMessageInPlace() {
        ConversationDetail conversation = aConversation();
        post(messageFrom(ALICE, conversation.getId(), "before")).block();
        Message photo = new Message("photo-1", ALICE, "Alice", null, conversation.getId(), "le podium",
                Instant.now(), Photo.stored("chat/" + conversation.getId() + "/photo-1.jpg", 1600, 1200));
        post(photo).block();

        adapter.markPhotoRemoved(conversation.getId(), "photo-1").block();

        StepVerifier.create(adapter.findById(conversation.getId(), BOB))
                .assertNext(read -> {
                    assertThat(read.getMessages()).hasSize(2);
                    Message marked = read.getMessages().get(1);
                    assertThat(marked.id()).isEqualTo("photo-1");
                    assertThat(marked.content()).isEqualTo("le podium");
                    assertThat(marked.photo()).isEqualTo(Photo.REMOVED);
                })
                .verifyComplete();
        StepVerifier.create(adapter.findUserConversations(BOB))
                .assertNext(summary -> assertThat(summary.lastMessage().photo()).isEqualTo(Photo.REMOVED))
                .verifyComplete();
    }

    @Test
    @DisplayName("a message is read alone from its bucket, photo removed once marked")
    void findMessage_shouldReadTheMessageAsStored() {
        ConversationDetail conversation = aConversation();
        post(messageFrom(ALICE, conversation.getId(), "before")).block();
        Message photo = new Message("photo-1", ALICE, "Alice", null, conversation.getId(), "le podium",
                Instant.now(), Photo.stored("chat/" + conversation.getId() + "/photo-1.jpg", 1600, 1200));
        post(photo).block();
        adapter.markPhotoRemoved(conversation.getId(), "photo-1").block();

        StepVerifier.create(adapter.findMessage(conversation.getId(), "photo-1"))
                .assertNext(read -> {
                    assertThat(read.id()).isEqualTo("photo-1");
                    assertThat(read.senderName()).isEqualTo("Alice");
                    assertThat(read.content()).isEqualTo("le podium");
                    assertThat(read.photo()).isEqualTo(Photo.REMOVED);
                })
                .verifyComplete();
        StepVerifier.create(adapter.findMessage(conversation.getId(), "missing")).verifyComplete();
    }

    // --- system messages --------------------------------------------------

    @Test
    @DisplayName("a notice is stored in the event's group as SYSTEM, read back as such, and listed in the preview")
    void postSystemMessage_shouldStoreASystemMessageInTheEventGroup() {
        adapter.saveEventGroup(GROUP).block();
        Message notice = Message.system("notice-1", "Date fixée", Instant.parse("2026-10-14T08:00:00Z"));

        StepVerifier.create(adapter.postSystemMessage("event-1", notice))
                .assertNext(posted -> {
                    assertThat(posted.duplicate()).isFalse();
                    assertThat(posted.participantIds()).containsExactly(ALICE, BOB, "carol");
                })
                .verifyComplete();

        StepVerifier.create(adapter.findByEventId("event-1", BOB))
                .assertNext(group -> {
                    assertThat(group.getMessages()).hasSize(1);
                    Message read = group.getMessages().getFirst();
                    assertThat(read.id()).isEqualTo("notice-1");
                    assertThat(read.type()).isEqualTo(MessageType.SYSTEM);
                    assertThat(read.senderId()).isEqualTo(Message.SYSTEM_SENDER_ID);
                })
                .verifyComplete();
        StepVerifier.create(adapter.findUserConversations(BOB))
                .assertNext(summary -> assertThat(summary.lastMessage().type()).isEqualTo(MessageType.SYSTEM))
                .verifyComplete();
    }

    @Test
    @DisplayName("the same notice posted twice is stored once")
    void postSystemMessage_shouldNotStoreTheSameNoticeTwice() {
        adapter.saveEventGroup(GROUP).block();
        Message notice = Message.system("notice-1", "Date fixée", Instant.now());
        adapter.postSystemMessage("event-1", notice).block();

        StepVerifier.create(adapter.postSystemMessage("event-1", notice))
                .assertNext(posted -> assertThat(posted.duplicate()).isTrue())
                .verifyComplete();
        StepVerifier.create(adapter.findByEventId("event-1", ALICE))
                .assertNext(group -> assertThat(group.getMessages()).hasSize(1))
                .verifyComplete();
    }

    @Test
    @DisplayName("a call's notice lands in a direct conversation too")
    void postSystemMessageToConversation_shouldStoreTheNoticeInADirectConversation() {
        ConversationDetail direct = adapter.saveWithInitialBucket(List.of(ALICE, BOB)).block();
        Message notice = Message.system("call-notice", "📞 Appel audio · 3 min", Instant.parse("2026-10-15T18:00:00Z"));

        StepVerifier.create(adapter.postSystemMessageToConversation(direct.getId(), notice))
                .assertNext(posted -> assertThat(posted.participantIds()).containsExactlyInAnyOrder(ALICE, BOB))
                .verifyComplete();
        StepVerifier.create(adapter.postSystemMessageToConversation(direct.getId(), notice))
                .assertNext(posted -> assertThat(posted.duplicate()).isTrue())
                .verifyComplete();
    }

    @Test
    void postSystemMessage_shouldBeEmpty_whenTheEventHasNoGroup() {
        StepVerifier.create(adapter.postSystemMessage("unknown", Message.system("n", "x", Instant.now())))
                .verifyComplete();
    }

    @Test
    @DisplayName("a text message keeps no type key in its bucket, and reads back as TEXT")
    void postMessage_shouldStoreNoTypeForAText() {
        ConversationDetail conversation = aConversation();
        post(messageFrom(ALICE, conversation.getId(), "hi")).block();

        org.bson.Document bucket = mongoTemplate.findOne(
                Query.query(Criteria.where("conversationId").is(conversation.getId())), org.bson.Document.class,
                "message_buckets").block();
        assertThat(bucket).isNotNull();
        assertThat(bucket.getList("messages", org.bson.Document.class).getFirst()).doesNotContainKey("ty");
        StepVerifier.create(adapter.findById(conversation.getId(), BOB))
                .assertNext(read -> assertThat(read.getMessages().getFirst().type()).isEqualTo(MessageType.TEXT))
                .verifyComplete();
    }

    // --- account erasure --------------------------------------------------

    private ConversationDetail conversationOf(String... participantIds) {
        return adapter.saveWithInitialBucket(List.of(participantIds)).block();
    }

    private List<Message> everyMessage(String conversationId, String reader) {
        return Flux.range(0, (int) bucketCount(conversationId))
                .concatMap(index -> adapter.findBucketByConversationIdAndBucketIndex(conversationId, index, reader))
                .flatMapIterable(bucket -> bucket.messages())
                .collectList()
                .block();
    }

    @Test
    @DisplayName("anonymizing clears the user's name in every bucket of every conversation, and nothing else")
    void anonymizeSender_shouldClearTheNameOnEveryMessageOfTheUser_inEveryBucket_andLeaveOthersAlone() {
        ConversationDetail withBob = conversationOf(ALICE, BOB);
        ConversationDetail withCarol = conversationOf(ALICE, "carol");
        // 60 identified messages: two buckets, alternating senders.
        Flux.range(0, 60)
                .concatMap(i -> post(new Message("m-" + i, i % 2 == 0 ? ALICE : BOB, i % 2 == 0 ? ALICE : BOB, BOB,
                        withBob.getId(), "message " + i, Instant.now())))
                .blockLast();
        Message photo = new Message("photo-1", ALICE, ALICE, null, withCarol.getId(), "le podium", Instant.now(),
                Photo.stored("chat/" + withCarol.getId() + "/photo-1.jpg", 1600, 1200));
        post(photo).block();
        post(messageFrom("carol", withCarol.getId(), "nice")).block();

        adapter.anonymizeSender(ALICE).block();

        List<Message> first = everyMessage(withBob.getId(), BOB);
        assertThat(bucketCount(withBob.getId())).isEqualTo(2);
        assertThat(first).hasSize(60);
        assertThat(first).filteredOn(message -> ALICE.equals(message.senderId()))
                .hasSize(30)
                .allSatisfy(message -> {
                    assertThat(message.senderName()).isNull();
                    assertThat(message.id()).isEqualTo("m-" + message.content().substring("message ".length()));
                });
        assertThat(first).filteredOn(message -> BOB.equals(message.senderId()))
                .extracting(Message::senderName).containsOnly(BOB);
        List<Message> second = everyMessage(withCarol.getId(), "carol");
        assertThat(second.getFirst().id()).isEqualTo("photo-1");
        assertThat(second.getFirst().senderName()).isNull();
        assertThat(second.getFirst().content()).isEqualTo("le podium");
        assertThat(second.getFirst().photo().key()).isEqualTo("chat/" + withCarol.getId() + "/photo-1.jpg");
        assertThat(second.get(1).senderName()).isEqualTo("carol");
    }

    @Test
    @DisplayName("the preview loses the name when the user's message is the last one, and only then")
    void anonymizeSender_shouldClearThePreview_whenTheUsersMessageIsTheLastOne() {
        ConversationDetail withBob = conversationOf(ALICE, BOB);
        ConversationDetail withCarol = conversationOf(ALICE, "carol");
        post(messageFrom(ALICE, withBob.getId(), "last from alice")).block();
        post(messageFrom(ALICE, withCarol.getId(), "hello")).block();
        post(messageFrom("carol", withCarol.getId(), "last from carol")).block();

        adapter.anonymizeSender(ALICE).block();

        StepVerifier.create(adapter.findUserConversations(ALICE).collectMap(summary -> summary.id()))
                .assertNext(byId -> {
                    Message bobPreview = byId.get(withBob.getId()).lastMessage();
                    assertThat(bobPreview.senderId()).isEqualTo(ALICE);
                    assertThat(bobPreview.senderName()).isNull();
                    assertThat(bobPreview.content()).isEqualTo("last from alice");
                    assertThat(byId.get(withCarol.getId()).lastMessage().senderName()).isEqualTo("carol");
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("a replayed erasure finds nothing left to change")
    void anonymizeSender_shouldModifyNothing_onReplay() {
        ConversationDetail withBob = conversationOf(ALICE, BOB);
        post(messageFrom(ALICE, withBob.getId(), "hi")).block();

        StepVerifier.create(messageBucketRepository.clearSenderName(ALICE)).expectNext(1L).verifyComplete();
        StepVerifier.create(conversationRepository.clearLastSenderName(ALICE)).expectNext(1L).verifyComplete();

        StepVerifier.create(messageBucketRepository.clearSenderName(ALICE)).expectNext(0L).verifyComplete();
        StepVerifier.create(conversationRepository.clearLastSenderName(ALICE)).expectNext(0L).verifyComplete();
    }

    @Test
    @DisplayName("each bucket is rewritten atomically: a message appended meanwhile is never lost")
    void anonymizeSender_shouldNotLoseAMessageAppendedConcurrently() {
        ConversationDetail withBob = conversationOf(ALICE, BOB);
        Flux.range(0, 20).concatMap(i -> post(messageFrom(ALICE, withBob.getId(), "before " + i))).blockLast();

        Flux.merge(
                        Flux.range(0, 20).flatMap(i -> post(messageFrom(BOB, withBob.getId(), "during " + i)), 4),
                        Flux.range(0, 5).concatMap(i -> adapter.anonymizeSender(ALICE)))
                .blockLast();

        List<Message> messages = everyMessage(withBob.getId(), BOB);
        assertThat(messages).hasSize(40);
        assertThat(messages).filteredOn(message -> ALICE.equals(message.senderId()))
                .extracting(Message::senderName).containsOnlyNulls();
        assertThat(messages).filteredOn(message -> BOB.equals(message.senderId()))
                .extracting(Message::senderName).containsOnly(BOB);
    }

    @Test
    @DisplayName("the user's messages are found through the multikey index, not by scanning every bucket")
    void anonymizeSender_shouldUseTheMessagesSenderIndex() {
        conversationOf(ALICE, BOB);

        StepVerifier.create(mongoTemplate.indexOps(MessageBucketEntity.class).getIndexInfo()
                        .map(index -> index.getName()).collectList())
                .assertNext(names -> assertThat(names).contains("messages_sender", "conversation_bucket_unique"))
                .verifyComplete();
        org.bson.Document plan = mongoTemplate.getCollection("message_buckets")
                .flatMap(collection -> reactor.core.publisher.Mono.from(collection
                        .find(new org.bson.Document("messages", new org.bson.Document("$elemMatch",
                                new org.bson.Document("s_id", ALICE).append("s_un", new org.bson.Document("$ne", null)))))
                        .explain()))
                .block();
        assertThat(plan.toJson()).contains("messages_sender");
    }

    @Test
    @DisplayName("only the user's private conversations become read-only, never an event group or someone else's")
    void markDirectConversationsReadOnly_shouldFlagOnlyTheUsersDirectConversations_notEventGroups() {
        ConversationDetail withBob = conversationOf(ALICE, BOB);
        ConversationDetail bobAndCarol = conversationOf(BOB, "carol");
        adapter.saveEventGroup(GROUP).block();

        adapter.markDirectConversationsReadOnly(ALICE).block();

        assertThat(adapter.findById(withBob.getId(), BOB).block().isReadOnly()).isTrue();
        assertThat(adapter.findById(bobAndCarol.getId(), BOB).block().isReadOnly()).isFalse();
        assertThat(adapter.findByEventId("event-1", BOB).block().isReadOnly()).isFalse();
        StepVerifier.create(conversationRepository.markDirectReadOnly(ALICE)).expectNext(0L).verifyComplete();
    }

    @Test
    @DisplayName("the list carries readOnly, false for every conversation stored without the flag")
    void findUserConversations_shouldExposeReadOnly_andDefaultToFalse() {
        ConversationDetail withAlice = conversationOf(ALICE, BOB);
        ConversationDetail withCarol = conversationOf(BOB, "carol");
        adapter.markDirectConversationsReadOnly(ALICE).block();

        StepVerifier.create(adapter.findUserConversations(BOB).collectMap(summary -> summary.id(), summary -> summary.readOnly()))
                .assertNext(readOnly -> {
                    assertThat(readOnly.get(withAlice.getId())).isTrue();
                    assertThat(readOnly.get(withCarol.getId())).isFalse();
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("the membership check reads the conversation, flag included, and nobody else's")
    void findMembership_shouldReturnTheConversationWithoutMessages_forAParticipantOnly() {
        ConversationDetail withBob = conversationOf(ALICE, BOB);
        post(messageFrom(ALICE, withBob.getId(), "hi")).block();
        adapter.markDirectConversationsReadOnly(BOB).block();

        StepVerifier.create(adapter.findMembership(withBob.getId(), ALICE))
                .assertNext(found -> {
                    assertThat(found.getParticipantIds()).containsExactly(ALICE, BOB);
                    assertThat(found.getMessages()).isNull();
                    assertThat(found.isReadOnly()).isTrue();
                })
                .verifyComplete();
        StepVerifier.create(adapter.findMembership(withBob.getId(), MALLORY)).verifyComplete();
    }

    private org.bson.Document storedGroup(String eventId) {
        return mongoTemplate.findOne(Query.query(Criteria.where("eventId").is(eventId)), org.bson.Document.class,
                "conversations").block();
    }

    @Test
    @DisplayName("a deleted event's group keeps only its event id and version: no title, no organizer")
    void closeEventGroup_shouldAlsoForgetTitleAndOrganizer_andKeepTheVersion() {
        adapter.saveEventGroup(new EventGroup("event-1", "Alice's birthday", ALICE, List.of(ALICE, BOB), 2)).block();

        adapter.closeEventGroup("event-1", 3).block();

        org.bson.Document closed = storedGroup("event-1");
        assertThat(closed).doesNotContainKeys("title", "organizerId", "lastMessage");
        assertThat(closed.getLong("groupVersion")).isEqualTo(3L);
        assertThat(closed.getList("participantIds", String.class)).isEmpty();
    }

    @Test
    @DisplayName("groups closed before this change lose the title the deleted organizer wrote; live ones keep it")
    void forgetClosedGroupsOrganizedBy_shouldUnsetTitleAndOrganizer_onClosedGroupsOnly() {
        adapter.saveEventGroup(new EventGroup("live", "Board games", ALICE, List.of(ALICE, BOB), 2)).block();
        mongoTemplate.insert(new ConversationEntity(null, List.of(), null, Instant.now(), "EVENT", "closed-by-alice",
                "Alice's birthday", 4L, ALICE, null)).block();
        mongoTemplate.insert(new ConversationEntity(null, List.of(), null, Instant.now(), "EVENT", "closed-by-bob",
                "Bob's party", 4L, BOB, null)).block();

        adapter.forgetClosedGroupsOrganizedBy(ALICE).block();

        assertThat(storedGroup("closed-by-alice")).doesNotContainKeys("title", "organizerId");
        assertThat(storedGroup("closed-by-alice").getLong("groupVersion")).isEqualTo(4L);
        assertThat(storedGroup("closed-by-bob").getString("title")).isEqualTo("Bob's party");
        assertThat(storedGroup("live").getString("title")).isEqualTo("Board games");
        assertThat(storedGroup("live").getString("organizerId")).isEqualTo(ALICE);
        StepVerifier.create(conversationRepository.forgetClosedGroupsOrganizedBy(ALICE)).expectNext(0L).verifyComplete();
    }
}
