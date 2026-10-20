package com.calendar.chat.infrastructure.messaging.adapters;

import com.calendar.chat.exception.GlobalErrorHandler;
import com.calendar.chat.exception.TechnicalErrorCounter;
import com.calendar.chat.infrastructure.messaging.models.CallEventBroadcastDTO;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.function.Function;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class CallEventConsumerTest {

    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    private double technicalErrors(String code, String source) {
        var counter = meterRegistry.find(GlobalErrorHandler.TECHNICAL_ERRORS_METRIC)
                .tags(GlobalErrorHandler.CODE_TAG, code, GlobalErrorHandler.SOURCE_TAG, source).counter();
        return counter == null ? 0 : counter.count();
    }

    private static final ObjectMapper JSON = JsonMapper.builder().build();
    private static final Instant AT = Instant.parse("2026-10-15T18:00:00Z");

    private KafkaCallBroadcaster broadcaster;
    private Function<Flux<Message<byte[]>>, Mono<Void>> consumer;

    @BeforeEach
    void setUp() {
        broadcaster = mock(KafkaCallBroadcaster.class);
        consumer = new CallEventConsumer(broadcaster, JSON, new TechnicalErrorCounter(meterRegistry)).callEvents();
    }

    private static CallEventBroadcastDTO payload(String recipientId) {
        return new CallEventBroadcastDTO("RINGING", recipientId, "alice",
                new CallEventBroadcastDTO.CallDTO("call-1", "conv-1", "alice", "Alice", "AUDIO", "RINGING", AT, null,
                        List.of("alice", recipientId), List.of()));
    }

    private static Message<byte[]> record(String recipientId) {
        return MessageBuilder.withPayload(JSON.writeValueAsBytes(payload(recipientId))).build();
    }

    @Test
    void callEvents_shouldDispatchEveryRecord() {
        StepVerifier.create(consumer.apply(Flux.just(record("bob"), record("carol")))).expectComplete().verify(Duration.ofSeconds(10));

        verify(broadcaster).accept(payload("bob"));
        verify(broadcaster).accept(payload("carol"));
    }

    @Test
    void callEvents_shouldKeepConsumingAfterAFailedRecord() {
        doThrow(new IllegalStateException("boom")).when(broadcaster).accept(payload("bob"));

        StepVerifier.create(consumer.apply(Flux.just(record("bob"), record("carol")))).expectComplete().verify(Duration.ofSeconds(10));

        verify(broadcaster).accept(payload("carol"));

        assertThat(technicalErrors(GlobalErrorHandler.UNEXPECTED_FAILURE_CODE, GlobalErrorHandler.SOURCE_KAFKA)).isEqualTo(1);
    }

    @Test
    void callEvents_shouldReadANullName_fromAnInstanceThatPredatesIt() {
        String older = """
                {"type":"RINGING","recipientId":"bob","actorId":"alice","call":{"id":"call-1",
                "conversationId":"conv-1","startedBy":"alice","media":"AUDIO","status":"RINGING",
                "startedAt":"2026-10-15T18:00:00Z","memberIds":["alice","bob"],"participants":[]}}""";

        StepVerifier.create(consumer.apply(Flux.just(MessageBuilder.withPayload(older.getBytes(StandardCharsets.UTF_8)).build()))).expectComplete().verify(Duration.ofSeconds(10));

        verify(broadcaster).accept(new CallEventBroadcastDTO("RINGING", "bob", "alice",
                new CallEventBroadcastDTO.CallDTO("call-1", "conv-1", "alice", null, "AUDIO", "RINGING", AT, null,
                        List.of("alice", "bob"), List.of())));
    }

    @Test
    void callEvents_shouldSkipARecordItCannotRead() {
        Message<byte[]> garbage = MessageBuilder.withPayload("not json".getBytes(StandardCharsets.UTF_8)).build();

        StepVerifier.create(consumer.apply(Flux.just(garbage, record("carol")))).expectComplete().verify(Duration.ofSeconds(10));

        verify(broadcaster, times(1)).accept(any());
        verify(broadcaster).accept(payload("carol"));

        assertThat(technicalErrors(GlobalErrorHandler.UNEXPECTED_FAILURE_CODE, GlobalErrorHandler.SOURCE_KAFKA)).isEqualTo(1);
    }

    @Test
    @DisplayName("nothing is read until the binder subscribes to the function's result")
    void callEvents_shouldDoNothingUntilTheBinderSubscribes() {
        consumer.apply(Flux.just(record("bob")));

        verifyNoInteractions(broadcaster);
    }
}
