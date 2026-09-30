package com.calendar.chat.infrastructure.messaging.adapters;

import com.calendar.chat.domain.models.Message;
import com.calendar.chat.domain.ports.MessageBroadcaster;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Sends a message through a real Kafka and waits for it to come back.
 *
 * <p>{@code KafkaMessageBroadcasterTest} beside this one mocks {@code StreamBridge}, so it
 * proves a record was handed to the binding and nothing more. Everything that can actually go
 * wrong lives past that point: whether the two binding names line up, whether the DTO survives
 * serialisation, whether the consumer is wired to the broadcaster at all. A typo in
 * {@code messageBroadcast-in-0} costs nothing at startup and silently stops every message.
 *
 * <p>The broker image matches the one the cluster runs, so the round trip is exercised against
 * the same Kafka version rather than a convenient one.
 *
 * <p>Opt-in: CI sets {@code CI=true}, locally pass {@code -Dintegration.tests=true}.
 */
@Testcontainers
@SpringBootTest(properties = {
        // The shared test profile selects in-process delivery, which is the whole thing this
        // class is not testing.
        "chat.broadcast.mode=kafka",
        // The in-cluster and containerised brokers speak PLAINTEXT; the SASL settings in
        // application.properties exist for a managed broker.
        "spring.kafka.properties.security.protocol=PLAINTEXT",
        "spring.cloud.stream.kafka.binder.configuration.security.protocol=PLAINTEXT",
        // The JAAS string in application.properties interpolates these two, and they are
        // injected by Kubernetes. Left unresolved, the binder built a malformed sasl.jaas.config
        // and failed to create its admin client — before PLAINTEXT ever made SASL irrelevant.
        // Values, not blanks: an empty mechanism is as invalid to Kafka as an unresolved one.
        "KAFKA_KEY=test",
        "KAFKA_SECRET=test",
        "spring.cloud.function.definition=messageBroadcast",
        "chat.broadcast.topic=MESSAGE_BROADCAST_IT",
        // The broker in the container creates the topic; production points at one where the
        // binder is not allowed to, hence the setting this overrides.
        "spring.cloud.stream.kafka.binder.auto-create-topics=true",
        // Without this the consumer starts at the latest offset, and a record produced before
        // the group has been assigned its partitions is simply never seen. Each instance gets a
        // fresh group by design, so there is never a committed offset to fall back on.
        "spring.cloud.stream.kafka.bindings.messageBroadcast-in-0.consumer.startOffset=earliest",
        "spring.cloud.stream.kafka.bindings.messageBroadcast-in-0.consumer.resetOffsets=true",
        // The shared test profile silences everything at WARN. A round trip that does not
        // happen is indistinguishable from one that is slow unless the binder says what it did.
        "logging.level.com.calendar.chat=DEBUG",
        "logging.level.org.springframework.cloud.stream=INFO",
        "logging.level.org.springframework.integration=INFO"
})
@ActiveProfiles("test")
@EnabledIf("containersRequested")
class KafkaMessageBroadcasterIntegrationTest {

    @Container
    @ServiceConnection
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.0");

    static boolean containersRequested() {
        return System.getenv("CI") != null || Boolean.getBoolean("integration.tests");
    }

    @Autowired
    private MessageBroadcaster broadcaster;

    private static Message to(String receiverId, String content) {
        return new Message("m-" + receiverId, "alice", "Alice", receiverId, "conv-1", content,
                LocalDateTime.now());
    }

    @Test
    @DisplayName("the broadcaster selected in kafka mode is the one that uses the broker")
    void context_shouldSelectTheKafkaBroadcaster() {
        assertThat(broadcaster).isInstanceOf(KafkaMessageBroadcaster.class);
    }

    @Test
    @DisplayName("a broadcast message comes back from the broker to the recipient's stream")
    void broadcast_shouldReachTheSubscriberThroughKafka() {
        // The full path: broadcast → messageBroadcast-out-0 → the topic → messageBroadcast-in-0
        // → MessageBroadcastConsumer → the inbound sink → subscribe(). Everything in the middle
        // is configuration that fails silently when it is wrong.
        StepVerifier.create(broadcaster.subscribe("bob"))
                .then(() -> broadcaster.broadcast(to("bob", "hello over kafka")).subscribe())
                .assertNext(message -> {
                    assertThat(message.receiverId()).isEqualTo("bob");
                    assertThat(message.content()).isEqualTo("hello over kafka");
                    assertThat(message.senderName()).isEqualTo("Alice");
                })
                .thenCancel()
                .verify(Duration.ofSeconds(60));
    }

    @Test
    @DisplayName("the message id survives the broker, as it must for the optimistic send to match")
    void broadcast_shouldPreserveTheMessageId() {
        // The frontend replaces its optimistic copy by matching on this id. Losing it across
        // the wire would show the message twice.
        StepVerifier.create(broadcaster.subscribe("carol"))
                .then(() -> broadcaster.broadcast(to("carol", "keep my id")).subscribe())
                .assertNext(message -> assertThat(message.id()).isEqualTo("m-carol"))
                .thenCancel()
                .verify(Duration.ofSeconds(60));
    }

    @Test
    @DisplayName("a subscriber only receives what is addressed to them")
    void subscribe_shouldFilterByRecipientAfterTheRoundTrip() {
        // Every instance receives every record — that is the point of a consumer group per pod.
        // The filtering happens on the way out, here, and not at the broker.
        StepVerifier.create(broadcaster.subscribe("dave"))
                .then(() -> {
                    broadcaster.broadcast(to("erin", "not for dave")).subscribe();
                    broadcaster.broadcast(to("dave", "for dave")).subscribe();
                })
                .assertNext(message -> assertThat(message.content()).isEqualTo("for dave"))
                .thenCancel()
                .verify(Duration.ofSeconds(60));
    }
}
