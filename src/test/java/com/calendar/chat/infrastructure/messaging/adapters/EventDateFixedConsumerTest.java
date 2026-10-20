package com.calendar.chat.infrastructure.messaging.adapters;

import com.calendar.chat.domain.models.Message;
import com.calendar.chat.domain.services.ChatService;
import com.calendar.chat.exception.GlobalErrorHandler;
import com.calendar.chat.exception.TechnicalErrorCounter;
import com.calendar.chat.infrastructure.messaging.notices.DateFixedNoticeFormatter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.messaging.support.MessageBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EventDateFixedConsumerTest {

    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    private double technicalErrors(String code, String source) {
        var counter = meterRegistry.find(GlobalErrorHandler.TECHNICAL_ERRORS_METRIC)
                .tags(GlobalErrorHandler.CODE_TAG, code, GlobalErrorHandler.SOURCE_TAG, source).counter();
        return counter == null ? 0 : counter.count();
    }

    private static final String SAME_DAY = "📅 Date fixée : jeudi 15 octobre, 19:00 – 21:00";

    @Mock
    private ChatService chatService;

    private EventDateFixedConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new EventDateFixedConsumer(chatService, new DateFixedNoticeFormatter(ZoneId.of("Europe/Paris")),
                JsonMapper.builder().build(), new TechnicalErrorCounter(meterRegistry));
    }

    private static org.springframework.messaging.Message<byte[]> record(String json) {
        return MessageBuilder.withPayload(json.getBytes(StandardCharsets.UTF_8)).build();
    }

    private static org.springframework.messaging.Message<byte[]> record(String eventId, String start, String end) {
        return record("""
                {"eventId":"%s","title":"Board games","start":%s,"end":%s,"organizerId":"olivia"}"""
                .formatted(eventId, start, end));
    }

    private static Mono<Message> posted() {
        return Mono.just(Message.system("n", SAME_DAY, Instant.EPOCH));
    }

    @Test
    @DisplayName("a record with ISO-8601 instants posts the formatted notice in its event's group")
    void eventDateFixed_shouldPostTheNotice_whenInstantsAreIsoStrings() {
        when(chatService.postSystemNotice(anyString(), anyString())).thenReturn(posted());

        StepVerifier.create(consumer.eventDateFixed().apply(Flux.just(
                record("e1", "\"2026-10-15T17:00:00Z\"", "\"2026-10-15T19:00:00Z\""),
                record("e2", "\"2026-10-15T17:00:00Z\"", "\"2026-10-15T19:00:00Z\"")))).expectComplete().verify(Duration.ofSeconds(10));

        verify(chatService, timeout(2000)).postSystemNotice("e1", SAME_DAY);
        verify(chatService, timeout(2000)).postSystemNotice("e2", SAME_DAY);
    }

    @Test
    @DisplayName("a record with numeric epoch instants, in seconds, reads the same")
    void eventDateFixed_shouldPostTheNotice_whenInstantsAreEpochNumbers() {
        when(chatService.postSystemNotice(anyString(), anyString())).thenReturn(posted());

        StepVerifier.create(consumer.eventDateFixed().apply(Flux.just(
                record("e1", "1792083600", "1792090800"),
                record("e2", "1792083600.000000000", "1792090800.000000000")))).expectComplete().verify(Duration.ofSeconds(10));

        verify(chatService, timeout(2000)).postSystemNotice("e1", SAME_DAY);
        verify(chatService, timeout(2000)).postSystemNotice("e2", SAME_DAY);
    }

    @Test
    @DisplayName("a field added by wely-events later does not make the record unreadable")
    void eventDateFixed_shouldIgnoreUnknownFields() {
        when(chatService.postSystemNotice(anyString(), anyString())).thenReturn(posted());

        StepVerifier.create(consumer.eventDateFixed().apply(Flux.just(record("""
                {"eventId":"e1","start":"2026-10-15T17:00:00Z","end":"2026-10-15T19:00:00Z","location":"Lyon"}""")))).expectComplete().verify(Duration.ofSeconds(10));

        verify(chatService, timeout(2000)).postSystemNotice("e1", SAME_DAY);
    }

    @Test
    @DisplayName("a move by the organizer posts the new date with the previous one")
    void eventDateFixed_shouldPostAMoveNotice_whenThePreviousDatesAreGiven() {
        when(chatService.postSystemNotice(anyString(), anyString())).thenReturn(posted());

        StepVerifier.create(consumer.eventDateFixed().apply(Flux.just(record("""
                {"eventId":"e1","title":"Bowling","start":"2026-10-15T18:00:00Z","end":"2026-10-15T20:00:00Z",
                 "organizerId":"olivia","previousStart":"2026-10-15T17:00:00Z","previousEnd":"2026-10-15T19:00:00Z"}""")))).expectComplete().verify(Duration.ofSeconds(10));

        verify(chatService, timeout(2000)).postSystemNotice("e1",
                "🔁 Sortie décalée : jeudi 15 octobre, 20:00 – 22:00 (au lieu de 19:00 – 21:00)");
    }

    @Test
    @DisplayName("an unreadable record is dropped, and the next one still posted")
    void eventDateFixed_shouldDropAnUnreadableRecord_andKeepConsuming() {
        when(chatService.postSystemNotice(eq("e2"), anyString())).thenReturn(posted());

        StepVerifier.create(consumer.eventDateFixed().apply(Flux.just(
                record("{not json"),
                record("e1", "\"next thursday\"", "null"),
                record("e2", "\"2026-10-15T17:00:00Z\"", "\"2026-10-15T19:00:00Z\"")))).expectComplete().verify(Duration.ofSeconds(10));

        verify(chatService, timeout(2000)).postSystemNotice("e2", SAME_DAY);
        verify(chatService, never()).postSystemNotice(eq("e1"), any());

        assertThat(technicalErrors(GlobalErrorHandler.UNEXPECTED_FAILURE_CODE, GlobalErrorHandler.SOURCE_KAFKA)).isEqualTo(2);
    }

    @Test
    @DisplayName("a failing notice, such as a group not created yet, is retried, then dropped without stopping the stream")
    void eventDateFixed_shouldRetryThenKeepConsuming() {
        AtomicInteger attempts = new AtomicInteger();
        when(chatService.postSystemNotice(eq("e1"), anyString())).thenReturn(Mono.defer(() -> {
            attempts.incrementAndGet();
            return Mono.error(new IllegalStateException("no group yet"));
        }));
        when(chatService.postSystemNotice(eq("e2"), anyString())).thenReturn(posted());

        StepVerifier.create(consumer.eventDateFixed().apply(Flux.just(
                record("e1", "\"2026-10-15T17:00:00Z\"", "\"2026-10-15T19:00:00Z\""),
                record("e2", "\"2026-10-15T17:00:00Z\"", "\"2026-10-15T19:00:00Z\"")))).expectComplete().verify(Duration.ofSeconds(10));

        // One attempt plus three retries with backoff from 1s, then the next record.
        verify(chatService, timeout(20_000)).postSystemNotice("e2", SAME_DAY);
        assertThat(attempts.get()).isEqualTo(4);

        assertThat(technicalErrors(GlobalErrorHandler.UNEXPECTED_FAILURE_CODE, GlobalErrorHandler.SOURCE_KAFKA)).isEqualTo(1);
    }

    @Test
    @DisplayName("a retry that succeeds posts the notice")
    void eventDateFixed_shouldPost_whenARetrySucceeds() throws InterruptedException {
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch posted = new CountDownLatch(1);
        when(chatService.postSystemNotice(eq("e1"), anyString())).thenReturn(Mono.defer(() ->
                attempts.incrementAndGet() == 1
                        ? Mono.error(new IllegalStateException("no group yet"))
                        : posted().doOnNext(notice -> posted.countDown())));

        StepVerifier.create(consumer.eventDateFixed().apply(Flux.just(
                record("e1", "\"2026-10-15T17:00:00Z\"", "\"2026-10-15T19:00:00Z\"")))).expectComplete().verify(Duration.ofSeconds(10));

        assertThat(posted.await(5, TimeUnit.SECONDS)).isTrue();
        // Each attempt calls the service afresh: a retry re-reads the group, which may exist by now.
        assertThat(attempts.get()).isEqualTo(2);
        verify(chatService, times(2)).postSystemNotice("e1", SAME_DAY);
    }

    @Test
    @DisplayName("nothing is read until the binder subscribes to the function's result")
    void eventDateFixed_shouldDoNothingUntilTheBinderSubscribes() {
        consumer.eventDateFixed().apply(Flux.just(record("e1", "\"2026-10-15T17:00:00Z\"", "\"2026-10-15T19:00:00Z\"")));

        verifyNoInteractions(chatService);
    }
}
