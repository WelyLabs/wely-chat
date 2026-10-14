package com.calendar.chat.infrastructure.messaging.adapters;

import com.calendar.chat.domain.models.Call;
import com.calendar.chat.domain.models.CallEvent;
import com.calendar.chat.domain.models.CallEventType;
import com.calendar.chat.domain.models.CallMedia;
import com.calendar.chat.domain.ports.CallBroadcaster;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A call event through a real Kafka and back, like {@link KafkaMessageBroadcasterIntegrationTest}
 * does for messages: the binding names, the DTO's serialisation and the consumer's wiring all fail
 * silently when wrong.
 *
 * <p>Opt-in: CI sets {@code CI=true}, locally pass {@code -Dintegration.tests=true}.
 */
@Testcontainers
@SpringBootTest(properties = {
        "chat.broadcast.mode=kafka",
        "spring.cloud.function.definition=callEvents",
        "chat.calls.topic=CALL_EVENTS_IT",
        "spring.cloud.stream.kafka.binder.auto-create-topics=true",
        "spring.cloud.stream.kafka.bindings.callEvents-in-0.consumer.startOffset=earliest",
        "spring.cloud.stream.kafka.bindings.callEvents-in-0.consumer.resetOffsets=true"
})
@ActiveProfiles("test")
@EnabledIf("containersRequested")
class KafkaCallBroadcasterIntegrationTest {

    @Container
    @ServiceConnection
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.0");

    @DynamicPropertySource
    static void bindTheBrokerToTheBinder(DynamicPropertyRegistry registry) {
        registry.add("spring.cloud.stream.kafka.binder.brokers", KAFKA::getBootstrapServers);
    }

    static boolean containersRequested() {
        return System.getenv("CI") != null || Boolean.getBoolean("integration.tests");
    }

    @Autowired
    private CallBroadcaster broadcaster;

    @Test
    @DisplayName("a call event comes back from the broker to its recipient, whole")
    void broadcast_shouldReachTheSubscriberThroughKafka() {
        assertThat(broadcaster).isInstanceOf(KafkaCallBroadcaster.class);
        CallEvent ringing = new CallEvent(CallEventType.RINGING, "bob", "alice",
                Call.ringing("conv-1", "alice", CallMedia.VIDEO, Instant.parse("2026-10-15T18:00:00Z"),
                        List.of("alice", "bob")));

        StepVerifier.create(broadcaster.subscribe("bob"))
                .then(() -> broadcaster.broadcast(ringing).subscribe())
                .expectNext(ringing)
                .thenCancel()
                .verify(Duration.ofSeconds(60));
    }
}
