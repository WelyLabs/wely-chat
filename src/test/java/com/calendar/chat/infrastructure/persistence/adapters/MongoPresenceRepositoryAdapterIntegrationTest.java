package com.calendar.chat.infrastructure.persistence.adapters;

import com.calendar.chat.domain.models.Presence;
import com.calendar.chat.infrastructure.persistence.models.entities.LastSeenEntity;
import com.calendar.chat.infrastructure.persistence.models.entities.PresenceConnectionEntity;
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
import org.bson.Document;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Presence against a real MongoDB: the "seen after" query, the heartbeat's multi-document update,
 * and the TTL index that clears the connections of a crashed instance.
 *
 * <p>Opt-in, like the other integration tests: CI sets {@code CI=true}, locally pass
 * {@code -Dintegration.tests=true}.
 */
@Testcontainers
@SpringBootTest(properties = "spring.data.mongodb.auto-index-creation=true")
@ActiveProfiles("test")
@EnabledIf("containersRequested")
class MongoPresenceRepositoryAdapterIntegrationTest {

    @Container
    @ServiceConnection
    static final MongoDBContainer MONGO = new MongoDBContainer("mongo:6.0");

    static boolean containersRequested() {
        return System.getenv("CI") != null || Boolean.getBoolean("integration.tests");
    }

    // Millisecond precision: what MongoDB stores, so instants compare equal on the way back.
    private static final Instant NOW = Instant.now().truncatedTo(ChronoUnit.MILLIS);

    @Autowired
    private MongoPresenceRepositoryAdapter adapter;

    @Autowired
    private ReactiveMongoTemplate mongoTemplate;

    @BeforeEach
    void clear() {
        mongoTemplate.remove(new Query(), PresenceConnectionEntity.class).block();
        mongoTemplate.remove(new Query(), LastSeenEntity.class).block();
    }

    @Test
    @DisplayName("a connected user is online, and seen now")
    void connect_shouldMakeTheUserOnline() {
        adapter.connect("c-1", "alice", "pod-1", NOW).block();

        assertThat(adapter.find("alice", NOW.minusSeconds(90)).block()).isEqualTo(new Presence("alice", true, NOW));
    }

    @Test
    @DisplayName("after the last disconnection the user is offline, last seen at that moment")
    void disconnect_shouldLeaveTheLastSeenTime() {
        adapter.connect("c-1", "alice", "pod-1", NOW.minusSeconds(600)).block();

        adapter.disconnect("c-1", "alice", NOW).block();

        assertThat(adapter.find("alice", NOW.minusSeconds(90)).block()).isEqualTo(new Presence("alice", false, NOW));
    }

    @Test
    @DisplayName("a second tab keeps the user online when the first closes")
    void disconnect_shouldKeepTheUserOnline_whileAnotherConnectionIsOpen() {
        adapter.connect("c-1", "alice", "pod-1", NOW).block();
        adapter.connect("c-2", "alice", "pod-2", NOW).block();

        adapter.disconnect("c-1", "alice", NOW).block();

        assertThat(adapter.find("alice", NOW.minusSeconds(90)).block().online()).isTrue();
    }

    @Test
    @DisplayName("a connection not refreshed within the window no longer counts")
    void find_shouldIgnoreAStaleConnection() {
        adapter.connect("c-1", "alice", "pod-1", NOW.minusSeconds(300)).block();

        assertThat(adapter.find("alice", NOW.minusSeconds(90)).block().online()).isFalse();
    }

    @Test
    @DisplayName("the heartbeat brings every refreshed connection back inside the window")
    void refresh_shouldKeepConnectionsAlive() {
        adapter.connect("c-1", "alice", "pod-1", NOW.minusSeconds(300)).block();
        adapter.connect("c-2", "bob", "pod-1", NOW.minusSeconds(300)).block();

        adapter.refresh(Map.of("c-1", "alice", "c-2", "bob"), NOW).block();

        assertThat(adapter.find("alice", NOW.minusSeconds(90)).block()).isEqualTo(new Presence("alice", true, NOW));
        assertThat(adapter.find("bob", NOW.minusSeconds(90)).block().online()).isTrue();
    }

    @Test
    @DisplayName("a user never connected is offline, never seen")
    void find_shouldAnswerForAnUnknownUser() {
        StepVerifier.create(adapter.find("nobody", NOW.minusSeconds(90)))
                .expectNext(new Presence("nobody", false, null))
                .verifyComplete();
    }

    @Test
    @DisplayName("connections carry a TTL index, so a crashed instance's users go offline for good")
    void connections_shouldExpireOnTheirOwn() {
        Document ttl = mongoTemplate.getCollection("presence_connections")
                .flatMapMany(collection -> collection.listIndexes())
                .filter(index -> index.containsKey("expireAfterSeconds"))
                .blockFirst(Duration.ofSeconds(10));

        assertThat(ttl).isNotNull();
        assertThat(ttl.get("key", Document.class)).containsKey("seenAt");
        assertThat(ttl.get("expireAfterSeconds", Number.class).longValue()).isEqualTo(120L);
    }
}
