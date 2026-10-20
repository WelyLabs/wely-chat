package com.calendar.chat.infrastructure.persistence.adapters;

import com.calendar.chat.domain.models.ConversationPhoto;
import com.calendar.chat.domain.models.Photo;
import com.calendar.chat.infrastructure.persistence.models.entities.ConversationPhotoEntity;
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
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The media tab's index against a real MongoDB: newest first, paged by date, one entry per message
 * (the unique index), and nothing from another conversation.
 *
 * <p>Opt-in: CI sets {@code CI=true}, locally pass {@code -Dintegration.tests=true}.
 */
@Testcontainers
@SpringBootTest(properties = "spring.data.mongodb.auto-index-creation=true")
@ActiveProfiles("test")
@EnabledIf("containersRequested")
class MongoConversationPhotoRepositoryAdapterIntegrationTest {

    @Container
    @ServiceConnection
    static final MongoDBContainer MONGO = new MongoDBContainer("mongo:6.0");

    static boolean containersRequested() {
        return System.getenv("CI") != null || Boolean.getBoolean("integration.tests");
    }

    @Autowired
    private MongoConversationPhotoRepositoryAdapter adapter;

    @Autowired
    private ReactiveMongoTemplate mongoTemplate;

    private static final Instant T0 = Instant.parse("2026-10-13T20:00:00Z");

    @BeforeEach
    void clear() {
        // Documents, not the collection: dropping takes the unique index with it.
        mongoTemplate.remove(new Query(), ConversationPhotoEntity.class).block();
    }

    private void save(String conversationId, String messageId, int minutesAfter) {
        adapter.save(new ConversationPhoto(conversationId, messageId, "alice",
                Photo.stored("chat/" + conversationId + "/" + messageId + ".jpg", 10, 10), T0.plusSeconds(60L * minutesAfter))).block();
    }

    @Test
    @DisplayName("pages newest first, the next page starting before the last photo shown")
    void findPage_shouldPageNewestFirst() {
        save("c1", "m1", 1);
        save("c1", "m2", 2);
        save("c1", "m3", 3);
        save("other", "m4", 4);

        StepVerifier.create(adapter.findPage("c1", null, 2).map(ConversationPhoto::messageId).collectList())
                .assertNext(page -> assertThat(page).containsExactly("m3", "m2"))
                .expectComplete().verify(Duration.ofSeconds(10));
        StepVerifier.create(adapter.findPage("c1", T0.plusSeconds(120), 2).map(ConversationPhoto::messageId).collectList())
                .assertNext(page -> assertThat(page).containsExactly("m1"))
                .expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void save_shouldKeepOneEntryPerMessage() {
        save("c1", "m1", 1);
        save("c1", "m1", 1);

        StepVerifier.create(adapter.findPage("c1", null, 10).count()).expectNext(1L).expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void find_andDelete_shouldNameTheConversation() {
        save("c1", "m1", 1);

        StepVerifier.create(adapter.find("other", "m1")).expectComplete().verify(Duration.ofSeconds(10));
        adapter.delete("other", "m1").block();
        StepVerifier.create(adapter.find("c1", "m1")).expectNextCount(1).expectComplete().verify(Duration.ofSeconds(10));

        adapter.delete("c1", "m1").block();
        StepVerifier.create(adapter.find("c1", "m1")).expectComplete().verify(Duration.ofSeconds(10));
    }
}
