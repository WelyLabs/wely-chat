package com.calendar.chat.infrastructure.messaging.adapters;

import com.calendar.chat.infrastructure.messaging.models.MessageBroadcastDTO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import reactor.core.publisher.Flux;

import java.time.LocalDateTime;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class MessageBroadcastConsumerTest {

    private KafkaMessageBroadcaster broadcaster;
    private Consumer<Flux<Message<MessageBroadcastDTO>>> consumer;

    @BeforeEach
    void setUp() {
        broadcaster = mock(KafkaMessageBroadcaster.class);
        consumer = new MessageBroadcastConsumer(broadcaster).messageBroadcast();
    }

    /** Fixed, so the same id always yields an equal payload and Mockito can match it. */
    private static final LocalDateTime SENT_AT = LocalDateTime.of(2025, 1, 1, 12, 0);

    private static MessageBroadcastDTO payload(String id) {
        return new MessageBroadcastDTO(id, "sender", "Sender", "alice", "conv-1", "hi", SENT_AT);
    }

    private static Message<MessageBroadcastDTO> record(String id) {
        return MessageBuilder.withPayload(payload(id)).build();
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
