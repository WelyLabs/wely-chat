package com.calendar.chat.infrastructure.messaging.adapters;

import com.calendar.chat.domain.models.ConversationDetail;
import com.calendar.chat.domain.models.DeletedUserMarker;
import com.calendar.chat.domain.models.Message;
import com.calendar.chat.domain.ports.ChatRepository;
import com.calendar.chat.domain.ports.DeletedUserMarkers;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.cloud.stream.binding.BindingService;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** USER_DELETED through real Kafka and MongoDB, opt-in with {@code -Dintegration.tests=true} or {@code CI=true}. */
@Testcontainers
@SpringBootTest(properties = {
        "chat.user-deleted.enabled=true",
        "spring.cloud.function.definition=userDeleted",
        "spring.cloud.stream.bindings.userDeleted-in-0.destination=USER_DELETED_IT",
        "spring.cloud.stream.kafka.binder.auto-create-topics=true",
        "spring.data.mongodb.auto-index-creation=true"
})
@ActiveProfiles("test")
@EnabledIf("containersRequested")
class UserDeletedConsumerIntegrationTest {

    @Container
    @ServiceConnection
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.0");

    @Container
    @ServiceConnection
    static final MongoDBContainer MONGO = new MongoDBContainer("mongo:6.0");

    @DynamicPropertySource
    static void bindTheBrokerToTheBinder(DynamicPropertyRegistry registry) {
        registry.add("spring.cloud.stream.kafka.binder.brokers", KAFKA::getBootstrapServers);
    }

    static boolean containersRequested() {
        return System.getenv("CI") != null || Boolean.getBoolean("integration.tests");
    }

    private static final String ALICE = "89a380b8-0000-4000-8000-00000000000a";
    private static final String BOB = "89a380b8-0000-4000-8000-00000000000b";
    private static final Instant DELETED_AT = Instant.parse("2026-10-15T14:03:27.123Z");

    @Autowired
    private BindingService bindingService;

    @Autowired
    private ChatRepository chatRepository;

    @Autowired
    private DeletedUserMarkers markers;

    private static void publish(String json) {
        Map<String, Object> config = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(config)) {
            producer.send(new ProducerRecord<>("USER_DELETED_IT", json.getBytes(StandardCharsets.UTF_8)));
            producer.flush();
        }
    }

    @Test
    @DisplayName("a record on USER_DELETED erases the account, through a binding with no output")
    void userDeleted_shouldBindWithoutAnOutputBinding_andEraseARecordPublishedToUSER_DELETED() {
        assertThat(bindingService.getConsumerBindingNames()).contains("userDeleted-in-0");
        assertThat(bindingService.getProducerBindingNames()).doesNotContain("userDeleted-out-0");
        ConversationDetail conversation = chatRepository.saveWithInitialBucket(List.of(ALICE, BOB)).block();
        chatRepository.postMessage(new Message(null, ALICE, "alice", BOB, conversation.getId(), "see you",
                Instant.now())).block();

        // An unreadable record first: it is skipped, and the stream goes on to the next.
        publish("not json");
        publish("{\"userId\":\"" + ALICE + "\",\"deletedAt\":\"2026-10-15T14:03:27.123Z\"}");

        await().atMost(Duration.ofSeconds(60)).until(() -> {
            DeletedUserMarker marker = markers.find(ALICE).block();
            return marker != null && marker.erasedAt() != null;
        });
        assertThat(markers.find(ALICE).block().deletedAt()).isEqualTo(DELETED_AT);
        ConversationDetail after = chatRepository.findById(conversation.getId(), BOB).block();
        assertThat(after.isReadOnly()).isTrue();
        assertThat(after.getMessages()).singleElement().satisfies(message -> {
            assertThat(message.senderId()).isEqualTo(ALICE);
            assertThat(message.senderName()).isNull();
            assertThat(message.content()).isEqualTo("see you");
        });
    }
}
