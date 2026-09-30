package com.calendar.chat.infrastructure.messaging.adapters;

import com.calendar.chat.domain.models.Message;
import com.calendar.chat.infrastructure.messaging.mappers.MessageBroadcastMapper;
import com.calendar.chat.infrastructure.messaging.mappers.MessageBroadcastMapperImpl;
import com.calendar.chat.infrastructure.messaging.models.MessageBroadcastDTO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.stream.function.StreamBridge;
import reactor.test.StepVerifier;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaMessageBroadcasterTest {

    private StreamBridge streamBridge;
    private KafkaMessageBroadcaster broadcaster;

    /**
     * The generated implementation rather than a mock: these tests care about the record
     * making a real round trip through the broker contract, which a stubbed mapper hides.
     */
    private final MessageBroadcastMapper mapper = new MessageBroadcastMapperImpl();

    @BeforeEach
    void setUp() {
        streamBridge = mock(StreamBridge.class);
        when(streamBridge.send(any(String.class), any())).thenReturn(true);
        broadcaster = new KafkaMessageBroadcaster(streamBridge, mapper);
    }

    private static Message to(String receiverId) {
        return new Message("m-" + receiverId, "sender", "Sender", receiverId, "conv-1", "hi",
                LocalDateTime.now());
    }

    @Test
    @DisplayName("broadcasting publishes to the outbound binding rather than to local subscribers")
    void broadcast_shouldPublishToTheBinding() {
        StepVerifier.create(broadcaster.broadcast(to("alice"))).verifyComplete();

        verify(streamBridge).send(eq(KafkaMessageBroadcaster.DESTINATION), any(MessageBroadcastDTO.class));
    }

    @Test
    @DisplayName("a message does not reach a local subscriber until it comes back from the broker")
    void broadcast_shouldNotShortCircuitTheBroker() {
        // The round trip matters: if broadcast fed the inbound sink directly, the sender's own
        // pod would deliver twice — once locally and once on consuming its own record.
        StepVerifier.create(broadcaster.subscribe("alice"))
                .then(() -> broadcaster.broadcast(to("alice")).subscribe())
                .expectNoEvent(java.time.Duration.ofMillis(50))
                .thenCancel()
                .verify();
    }

    @Test
    @DisplayName("a refused send completes rather than failing the caller")
    void broadcast_shouldCompleteWhenTheBindingRefusesTheRecord() {
        when(streamBridge.send(any(String.class), any())).thenReturn(false);

        // Deliberate: the message is already in MongoDB by the time this runs, so raising here
        // would report a failed send for a message that was in fact saved. The live update is
        // lost and logged; the client sees it on the next load.
        StepVerifier.create(broadcaster.broadcast(to("alice"))).verifyComplete();
    }

    @Test
    @DisplayName("a consumed record reaches the subscriber it is addressed to")
    void accept_shouldDeliverToTheAddressedSubscriber() {
        StepVerifier.create(broadcaster.subscribe("alice"))
                .then(() -> {
                    broadcaster.accept(mapper.toBroadcastDTO(to("bob")));
                    broadcaster.accept(mapper.toBroadcastDTO(to("alice")));
                })
                .assertNext(message -> assertThat(message.receiverId()).isEqualTo("alice"))
                .thenCancel()
                .verify();
    }

    @Test
    @DisplayName("a consumed record with no recipient does not kill the stream")
    void accept_shouldTolerateARecordWithoutARecipient() {
        MessageBroadcastDTO withoutRecipient = mapper.toBroadcastDTO(
                new Message("m-1", "sender", "Sender", null, "conv-1", "hi", LocalDateTime.now()));

        StepVerifier.create(broadcaster.subscribe("alice"))
                .then(() -> {
                    broadcaster.accept(withoutRecipient);
                    broadcaster.accept(mapper.toBroadcastDTO(to("alice")));
                })
                .assertNext(message -> assertThat(message.receiverId()).isEqualTo("alice"))
                .thenCancel()
                .verify();
    }

    @Test
    @DisplayName("consuming still works after the last subscriber has left")
    void subscribe_shouldOutliveItsSubscribers() {
        StepVerifier.create(broadcaster.subscribe("alice"))
                .then(() -> broadcaster.accept(mapper.toBroadcastDTO(to("alice"))))
                .expectNextCount(1)
                .thenCancel()
                .verify();

        StepVerifier.create(broadcaster.subscribe("alice"))
                .then(() -> broadcaster.accept(mapper.toBroadcastDTO(to("alice"))))
                .expectNextCount(1)
                .thenCancel()
                .verify();
    }
}
