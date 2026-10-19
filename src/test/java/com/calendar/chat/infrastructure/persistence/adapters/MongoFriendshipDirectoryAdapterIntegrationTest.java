package com.calendar.chat.infrastructure.persistence.adapters;

import com.calendar.chat.domain.models.Friendship;
import com.calendar.chat.infrastructure.persistence.models.entities.FriendshipEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

/** The friendships projection against a real MongoDB, opt-in with {@code -Dintegration.tests=true} or {@code CI=true}. */
@Testcontainers
@SpringBootTest(properties = "spring.data.mongodb.auto-index-creation=true")
@ActiveProfiles("test")
@EnabledIf("containersRequested")
class MongoFriendshipDirectoryAdapterIntegrationTest {

    @Container
    @ServiceConnection
    static final MongoDBContainer MONGO = new MongoDBContainer("mongo:6.0");

    static boolean containersRequested() {
        return System.getenv("CI") != null || Boolean.getBoolean("integration.tests");
    }

    private static final String ALICE = "11111111-0000-4000-8000-000000000001";
    private static final String BOB = "22222222-0000-4000-8000-000000000002";
    private static final String CAROL = "33333333-0000-4000-8000-000000000003";

    @Autowired
    private MongoFriendshipDirectoryAdapter adapter;

    @Autowired
    private ReactiveMongoTemplate mongoTemplate;

    @BeforeEach
    void clear() {
        mongoTemplate.remove(new Query(), FriendshipEntity.class).block();
    }

    @Test
    void saveIfNewer_shouldMakeThemFriends_whicheverSideIsAsked() {
        StepVerifier.create(adapter.saveIfNewer(new Friendship(BOB, ALICE, 1))).expectNext(true).verifyComplete();

        StepVerifier.create(adapter.areFriends(ALICE, BOB)).expectNext(true).verifyComplete();
        StepVerifier.create(adapter.areFriends(BOB, ALICE)).expectNext(true).verifyComplete();
        StepVerifier.create(adapter.areFriends(ALICE, CAROL)).expectNext(false).verifyComplete();
    }

    @Test
    @DisplayName("an ended pair keeps its version: a replayed older value cannot make them friends again")
    void end_shouldKeepTheVersion_soAnOlderValueCannotReviveThePair() {
        adapter.saveIfNewer(new Friendship(ALICE, BOB, 5)).block();

        StepVerifier.create(adapter.end(ALICE, BOB)).expectNext(true).verifyComplete();
        StepVerifier.create(adapter.end(ALICE, BOB)).expectNext(false).verifyComplete();
        StepVerifier.create(adapter.areFriends(ALICE, BOB)).expectNext(false).verifyComplete();

        StepVerifier.create(adapter.saveIfNewer(new Friendship(ALICE, BOB, 5))).expectNext(false).verifyComplete();
        StepVerifier.create(adapter.areFriends(ALICE, BOB)).expectNext(false).verifyComplete();

        StepVerifier.create(adapter.saveIfNewer(new Friendship(ALICE, BOB, 6))).expectNext(true).verifyComplete();
        StepVerifier.create(adapter.areFriends(ALICE, BOB)).expectNext(true).verifyComplete();
    }

    @Test
    void end_shouldAnswerFalse_forAPairNeverStored() {
        StepVerifier.create(adapter.end(ALICE, CAROL)).expectNext(false).verifyComplete();
    }

    @Test
    void forget_shouldRemoveEveryPairOfTheAccount_andNoOther() {
        adapter.saveIfNewer(new Friendship(ALICE, BOB, 1)).block();
        adapter.saveIfNewer(new Friendship(CAROL, ALICE, 1)).block();
        adapter.saveIfNewer(new Friendship(BOB, CAROL, 1)).block();

        StepVerifier.create(adapter.forget(ALICE)).verifyComplete();

        assertThat(mongoTemplate.findAll(FriendshipEntity.class).map(FriendshipEntity::getId).collectList().block())
                .containsExactly(Friendship.pairIdOf(BOB, CAROL));
    }
}
