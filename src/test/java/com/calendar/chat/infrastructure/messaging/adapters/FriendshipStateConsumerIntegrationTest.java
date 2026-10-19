package com.calendar.chat.infrastructure.messaging.adapters;

import com.calendar.chat.domain.ports.FriendshipDirectory;
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
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** FRIENDSHIP_STATE through real Kafka and MongoDB, opt-in with {@code -Dintegration.tests=true} or {@code CI=true}. */
@Testcontainers
@SpringBootTest(properties = {
        "chat.friendship-state.enabled=true",
        "spring.cloud.function.definition=friendshipState",
        "spring.cloud.stream.bindings.friendshipState-in-0.destination=FRIENDSHIP_STATE_IT",
        "spring.cloud.stream.kafka.binder.auto-create-topics=true",
        "spring.data.mongodb.auto-index-creation=true"
})
@ActiveProfiles("test")
@EnabledIf("containersRequested")
class FriendshipStateConsumerIntegrationTest {

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

    private static final String ALICE = "11111111-0000-4000-8000-000000000001";
    private static final String BOB = "22222222-0000-4000-8000-000000000002";
    private static final String KEY = ALICE + ":" + BOB;

    @Autowired
    private BindingService bindingService;

    @Autowired
    private FriendshipDirectory friendships;

    /** A null value is a tombstone, as wely-social sends when a friendship ends. */
    private static void publish(String json) {
        Map<String, Object> config = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(config)) {
            producer.send(new ProducerRecord<>("FRIENDSHIP_STATE_IT", KEY.getBytes(StandardCharsets.UTF_8),
                    json == null ? null : json.getBytes(StandardCharsets.UTF_8)));
            producer.flush();
        }
    }

    @Test
    @DisplayName("a value makes the pair friends, and the tombstone that follows ends it")
    void friendshipState_shouldApplyAValue_thenATombstone_publishedToFRIENDSHIP_STATE() {
        assertThat(bindingService.getConsumerBindingNames()).contains("friendshipState-in-0");
        assertThat(bindingService.getProducerBindingNames()).doesNotContain("friendshipState-out-0");

        // An unreadable record first: it is dropped, and the stream goes on to the next.
        publish("not json");
        publish("{\"a\":\"%s\",\"b\":\"%s\",\"friends\":true,\"closeness\":\"FRIEND\",\"version\":3}".formatted(ALICE, BOB));
        await().atMost(Duration.ofSeconds(60)).until(() -> Boolean.TRUE.equals(friendships.areFriends(BOB, ALICE).block()));

        publish(null);
        await().atMost(Duration.ofSeconds(60)).until(() -> Boolean.FALSE.equals(friendships.areFriends(ALICE, BOB).block()));
    }
}
