package com.calendar.chat.infrastructure.messaging.adapters;

import com.calendar.chat.infrastructure.messaging.models.CallEventBroadcastDTO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import reactor.core.publisher.Flux;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class CallEventConsumerTest {

    private static final ObjectMapper JSON = JsonMapper.builder().build();
    private static final Instant AT = Instant.parse("2026-10-15T18:00:00Z");

    private KafkaCallBroadcaster broadcaster;
    private Consumer<Flux<Message<byte[]>>> consumer;

    @BeforeEach
    void setUp() {
        broadcaster = mock(KafkaCallBroadcaster.class);
        consumer = new CallEventConsumer(broadcaster, JSON).callEvents();
    }

    private static CallEventBroadcastDTO payload(String recipientId) {
        return new CallEventBroadcastDTO("RINGING", recipientId, "alice",
                new CallEventBroadcastDTO.CallDTO("call-1", "conv-1", "alice", "AUDIO", "RINGING", AT, null,
                        List.of("alice", recipientId), List.of()));
    }

    private static Message<byte[]> record(String recipientId) {
        return MessageBuilder.withPayload(JSON.writeValueAsBytes(payload(recipientId))).build();
    }

    @Test
    void callEvents_shouldDispatchEveryRecord() {
        consumer.accept(Flux.just(record("bob"), record("carol")));

        verify(broadcaster).accept(payload("bob"));
        verify(broadcaster).accept(payload("carol"));
    }

    @Test
    void callEvents_shouldKeepConsumingAfterAFailedRecord() {
        doThrow(new IllegalStateException("boom")).when(broadcaster).accept(payload("bob"));

        consumer.accept(Flux.just(record("bob"), record("carol")));

        verify(broadcaster).accept(payload("carol"));
    }

    @Test
    void callEvents_shouldSkipARecordItCannotRead() {
        Message<byte[]> garbage = MessageBuilder.withPayload("not json".getBytes(StandardCharsets.UTF_8)).build();

        consumer.accept(Flux.just(garbage, record("carol")));

        verify(broadcaster, times(1)).accept(any());
        verify(broadcaster).accept(payload("carol"));
    }
}
