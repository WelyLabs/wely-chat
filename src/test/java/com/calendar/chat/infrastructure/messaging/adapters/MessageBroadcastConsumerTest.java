package com.calendar.chat.infrastructure.messaging.adapters;

import com.calendar.chat.infrastructure.messaging.models.MessageBroadcastDTO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import reactor.core.publisher.Mono;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;

import java.time.Instant;
import java.util.function.Function;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class MessageBroadcastConsumerTest {

    private static final ObjectMapper JSON = JsonMapper.builder().build();

    private KafkaMessageBroadcaster broadcaster;
    private Function<Flux<Message<byte[]>>, Mono<Void>> consumer;

    @BeforeEach
    void setUp() {
        broadcaster = mock(KafkaMessageBroadcaster.class);
        consumer = new MessageBroadcastConsumer(broadcaster, JSON).messageBroadcast();
    }

    /** Fixed, so the same id always yields an equal payload and Mockito can match it. */
    private static final Instant SENT_AT = Instant.parse("2025-01-01T12:00:00Z");

    private static MessageBroadcastDTO payload(String id) {
        return new MessageBroadcastDTO(id, "sender", "Sender", "alice", "conv-1", "hi", SENT_AT, null, null, "NEW");
    }

    private static Message<byte[]> record(String id) {
        return MessageBuilder.withPayload(JSON.writeValueAsBytes(payload(id))).build();
    }

    private static Message<byte[]> raw(String json) {
        return MessageBuilder.withPayload(json.getBytes(StandardCharsets.UTF_8)).build();
    }

    @Test
    @DisplayName("each record's payload is handed to the broadcaster")
    void messageBroadcast_shouldDispatchEveryRecord() {
        StepVerifier.create(consumer.apply(Flux.just(record("m-1"), record("m-2")))).verifyComplete();

        verify(broadcaster).accept(payload("m-1"));
        verify(broadcaster).accept(payload("m-2"));
    }

    @Test
    @DisplayName("one failing record does not stop the ones behind it")
    void messageBroadcast_shouldKeepConsumingAfterAFailedRecord() {
        // The point of catching per record: a cancelled subscription would leave the instance
        // silently deaf until it restarts, which is the failure mode this guards against.
        doThrow(new IllegalStateException("boom")).when(broadcaster).accept(payload("m-1"));

        StepVerifier.create(consumer.apply(Flux.just(record("m-1"), record("m-2")))).verifyComplete();

        verify(broadcaster).accept(payload("m-2"));
    }

    @Test
    @DisplayName("an unreadable record is dropped and the stream goes on: no pod goes deaf over one record")
    void messageBroadcast_shouldSkipARecordItCannotRead() {
        // The record that took every pod down after the switch to instants: a time without a zone.
        String oldFormat = "{\"id\":\"m-0\",\"senderId\":\"sender\",\"senderName\":\"Sender\","
                + "\"receiverId\":\"alice\",\"conversationId\":\"conv-1\",\"content\":\"hi\","
                + "\"timestamp\":\"2026-10-12T23:44:46\"}";

        StepVerifier.create(consumer.apply(Flux.just(raw(oldFormat), raw("not json"), record("m-1")))).verifyComplete();

        verify(broadcaster).accept(payload("m-1"));
        verify(broadcaster, times(1)).accept(any());
    }

    @Test
    @DisplayName("a record written before kinds existed is read, without a kind")
    void messageBroadcast_shouldReadARecordWithoutKind() {
        String legacy = "{\"id\":\"m-0\",\"senderId\":\"sender\",\"senderName\":\"Sender\","
                + "\"receiverId\":\"alice\",\"conversationId\":\"conv-1\",\"content\":\"hi\","
                + "\"timestamp\":\"2025-01-01T12:00:00Z\"}";

        StepVerifier.create(consumer.apply(Flux.just(raw(legacy)))).verifyComplete();

        verify(broadcaster).accept(new MessageBroadcastDTO("m-0", "sender", "Sender", "alice", "conv-1", "hi",
                SENT_AT, null, null, null));
    }

    @Test
    @DisplayName("a stream-level failure reaches the binder that subscribed, instead of being swallowed here")
    void messageBroadcast_shouldPropagateAStreamFailure() {
        // Not the same case as a failing record: a connection drop belongs to the binder, which
        // subscribes to the function's result, so it must come out of it untouched.
        StepVerifier.create(consumer.apply(Flux.error(new IllegalStateException("broker gone"))))
                .verifyErrorMessage("broker gone");

        verifyNoInteractions(broadcaster);
    }

    @Test
    @DisplayName("nothing is read until the binder subscribes to the function's result")
    void messageBroadcast_shouldDoNothingUntilTheBinderSubscribes() {
        consumer.apply(Flux.just(record("m-1")));

        verifyNoInteractions(broadcaster);
    }
}
