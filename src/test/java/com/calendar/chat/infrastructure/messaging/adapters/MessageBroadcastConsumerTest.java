package com.calendar.chat.infrastructure.messaging.adapters;

import com.calendar.chat.infrastructure.messaging.models.MessageBroadcastDTO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import reactor.core.publisher.Flux;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;

import java.time.Instant;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class MessageBroadcastConsumerTest {

    private static final ObjectMapper JSON = JsonMapper.builder().build();

    private KafkaMessageBroadcaster broadcaster;
    private Consumer<Flux<Message<byte[]>>> consumer;

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
        consumer.accept(Flux.just(record("m-1"), record("m-2")));

        verify(broadcaster).accept(payload("m-1"));
        verify(broadcaster).accept(payload("m-2"));
    }

    @Test
    @DisplayName("one failing record does not stop the ones behind it")
    void messageBroadcast_shouldKeepConsumingAfterAFailedRecord() {
        // The point of onErrorContinue here: a cancelled subscription would leave the instance
        // silently deaf until it restarts, which is the failure mode this guards against.
        doThrow(new IllegalStateException("boom")).when(broadcaster).accept(payload("m-1"));

        consumer.accept(Flux.just(record("m-1"), record("m-2")));

        verify(broadcaster).accept(payload("m-2"));
    }

    @Test
    @DisplayName("an unreadable record is dropped and the stream goes on: no pod goes deaf over one record")
    void messageBroadcast_shouldSkipARecordItCannotRead() {
        // The record that took every pod down after the switch to instants: a time without a zone.
        String oldFormat = "{\"id\":\"m-0\",\"senderId\":\"sender\",\"senderName\":\"Sender\","
                + "\"receiverId\":\"alice\",\"conversationId\":\"conv-1\",\"content\":\"hi\","
                + "\"timestamp\":\"2026-10-12T23:44:46\"}";

        consumer.accept(Flux.just(raw(oldFormat), raw("not json"), record("m-1")));

        verify(broadcaster).accept(payload("m-1"));
        verify(broadcaster, times(1)).accept(any());
    }

    @Test
    @DisplayName("a record written before kinds existed is read, without a kind")
    void messageBroadcast_shouldReadARecordWithoutKind() {
        String legacy = "{\"id\":\"m-0\",\"senderId\":\"sender\",\"senderName\":\"Sender\","
                + "\"receiverId\":\"alice\",\"conversationId\":\"conv-1\",\"content\":\"hi\","
                + "\"timestamp\":\"2025-01-01T12:00:00Z\"}";

        consumer.accept(Flux.just(raw(legacy)));

        verify(broadcaster).accept(new MessageBroadcastDTO("m-0", "sender", "Sender", "alice", "conv-1", "hi",
                SENT_AT, null, null, null));
    }

    @Test
    @DisplayName("a stream-level failure is handled, not rethrown at the subscriber")
    void messageBroadcast_shouldHandleAStreamFailure() {
        // This is not the same case as the one above. onErrorContinue, which this consumer used
        // to rely on, rescues only errors it can attribute to an element; a connection drop or
        // a payload the binder cannot convert reaches subscribe() instead, and with no error
        // handler there Reactor rethrows it as ErrorCallbackNotImplemented.
        assertThatCode(() -> consumer.accept(Flux.error(new IllegalStateException("broker gone"))))
                .doesNotThrowAnyException();
    }
}
