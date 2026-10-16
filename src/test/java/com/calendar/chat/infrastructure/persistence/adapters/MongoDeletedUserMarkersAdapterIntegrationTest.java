package com.calendar.chat.infrastructure.persistence.adapters;

import com.calendar.chat.domain.models.DeletedUserMarker;
import com.calendar.chat.infrastructure.persistence.models.entities.DeletedUserMarkerEntity;
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

import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** The deleted-account markers against a real MongoDB, opt-in with {@code -Dintegration.tests=true} or {@code CI=true}. */
@Testcontainers
@SpringBootTest(properties = "spring.data.mongodb.auto-index-creation=true")
@ActiveProfiles("test")
@EnabledIf("containersRequested")
class MongoDeletedUserMarkersAdapterIntegrationTest {

    @Container
    @ServiceConnection
    static final MongoDBContainer MONGO = new MongoDBContainer("mongo:6.0");

    static boolean containersRequested() {
        return System.getenv("CI") != null || Boolean.getBoolean("integration.tests");
    }

    private static final Instant FIRST = Instant.parse("2026-10-15T14:03:27.123Z");
    private static final Instant REPLAYED = Instant.parse("2026-10-22T09:00:00Z");
    private static final Instant ERASED = Instant.parse("2026-10-15T14:03:29Z");

    @Autowired
    private MongoDeletedUserMarkersAdapter adapter;

    @Autowired
    private ReactiveMongoTemplate mongoTemplate;

    @BeforeEach
    void clear() {
        mongoTemplate.remove(new Query(), DeletedUserMarkerEntity.class).block();
    }

    @Test
    @DisplayName("a replay a week later keeps the first deletedAt, and the erasedAt set since")
    void markDeleted_shouldKeepTheFirstDeletedAt_whenWrittenTwice() {
        adapter.markDeleted("alice", FIRST).block();
        adapter.markErased("alice", ERASED).block();

        StepVerifier.create(adapter.markDeleted("alice", REPLAYED)).verifyComplete();

        StepVerifier.create(adapter.find("alice"))
                .expectNext(new DeletedUserMarker("alice", FIRST, ERASED))
                .verifyComplete();
        assertThat(mongoTemplate.count(new Query(), DeletedUserMarkerEntity.class).block()).isEqualTo(1L);
    }

    @Test
    void findDeletedAmong_shouldReturnOnlyMarkedIds() {
        adapter.markDeleted("alice", FIRST).block();
        adapter.markDeleted("carol", FIRST).block();

        StepVerifier.create(adapter.findDeletedAmong(List.of("alice", "bob", "carol")).collectList())
                .assertNext(ids -> assertThat(ids).containsExactlyInAnyOrder("alice", "carol"))
                .verifyComplete();
        StepVerifier.create(adapter.isDeleted("bob")).expectNext(false).verifyComplete();
        StepVerifier.create(adapter.isDeleted("alice")).expectNext(true).verifyComplete();
    }

    @Test
    @DisplayName("a marker without erasedAt says an erasure is still owed")
    void find_shouldReadAMarkerNotErasedYet() {
        adapter.markDeleted("alice", FIRST).block();

        StepVerifier.create(adapter.find("alice"))
                .expectNext(new DeletedUserMarker("alice", FIRST, null))
                .verifyComplete();
        StepVerifier.create(adapter.find("bob")).verifyComplete();
    }

    @Test
    @DisplayName("only markers whose erasure ran are listed: none, or one still owed, means reconciliation erases")
    void erasedAmong_shouldReturnOnlyIdsWithErasedAt() {
        adapter.markDeleted("alice", FIRST).block();
        adapter.markErased("alice", ERASED).block();
        adapter.markDeleted("carol", FIRST).block();
        adapter.markDeleted("dave", FIRST).block();
        adapter.markErased("dave", ERASED).block();

        StepVerifier.create(adapter.erasedAmong(List.of("alice", "bob", "carol")))
                .expectNext(Set.of("alice"))
                .verifyComplete();
        StepVerifier.create(adapter.erasedAmong(List.of())).expectNext(Set.of()).verifyComplete();
    }
}
