package com.calendar.chat.infrastructure.messaging.adapters;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.calendar.chat.domain.services.AccountErasureService;
import com.calendar.chat.exception.BusinessErrorCode;
import com.calendar.chat.exception.BusinessException;
import com.calendar.chat.exception.GlobalErrorHandler;
import com.calendar.chat.exception.TechnicalErrorCode;
import com.calendar.chat.exception.TechnicalErrorCounter;
import com.calendar.chat.exception.TechnicalException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class UserDeletedConsumerTest {

    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    private double technicalErrors(String code, String source) {
        var counter = meterRegistry.find(GlobalErrorHandler.TECHNICAL_ERRORS_METRIC)
                .tags(GlobalErrorHandler.CODE_TAG, code, GlobalErrorHandler.SOURCE_TAG, source).counter();
        return counter == null ? 0 : counter.count();
    }

    private static final String USER = "89a380b8-0000-4000-8000-000000000001";
    private static final String OTHER = "89a380b8-0000-4000-8000-000000000002";
    private static final Instant DELETED_AT = Instant.parse("2026-10-15T14:03:27.123Z");

    @Mock
    private AccountErasureService erasure;

    private UserDeletedConsumer consumer;
    private final ListAppender<ILoggingEvent> logs = new ListAppender<>();

    @BeforeEach
    void setUp() {
        consumer = new UserDeletedConsumer(erasure, JsonMapper.builder().build(), new TechnicalErrorCounter(meterRegistry));
        logs.start();
        consumerLogger().addAppender(logs);
    }

    @AfterEach
    void tearDown() {
        consumerLogger().detachAppender(logs);
    }

    private static Logger consumerLogger() {
        return (Logger) LoggerFactory.getLogger(UserDeletedConsumer.class);
    }

    private static Message<byte[]> record(String json) {
        return MessageBuilder.withPayload(json.getBytes(StandardCharsets.UTF_8)).build();
    }

    private static Message<byte[]> deletionOf(String userId) {
        return record("{\"userId\":\"%s\",\"deletedAt\":\"2026-10-15T14:03:27.123Z\"}".formatted(userId));
    }

    @Test
    @DisplayName("deletedAt arrives as an ISO-8601 string, the contract's format")
    void userDeleted_shouldEraseTheUser_withTheRecordsDeletedAt() {
        when(erasure.erase(USER, DELETED_AT)).thenReturn(Mono.empty());

        StepVerifier.create(consumer.userDeleted().apply(Flux.just(deletionOf(USER)))).expectComplete().verify(Duration.ofSeconds(10));

        verify(erasure).erase(USER, DELETED_AT);
    }

    @Test
    @DisplayName("a record sent before deletedAt existed carries only the userId")
    void userDeleted_shouldPassANullDeletedAt_whenTheRecordHasNone() {
        when(erasure.erase(USER, null)).thenReturn(Mono.empty());

        StepVerifier.create(consumer.userDeleted().apply(Flux.just(record("{\"userId\":\"" + USER + "\"}"))))
                .expectComplete().verify(Duration.ofSeconds(10));

        verify(erasure).erase(USER, null);
    }

    @Test
    void userDeleted_shouldIgnoreUnknownFields() {
        when(erasure.erase(USER, DELETED_AT)).thenReturn(Mono.empty());

        StepVerifier.create(consumer.userDeleted().apply(Flux.just(record(
                        "{\"userId\":\"" + USER + "\",\"deletedAt\":\"2026-10-15T14:03:27.123Z\",\"reason\":\"USER\"}"))))
                .expectComplete().verify(Duration.ofSeconds(10));

        verify(erasure).erase(USER, DELETED_AT);
    }

    @Test
    @DisplayName("an unreadable record is skipped: the next one is still erased")
    void userDeleted_shouldSkipAnUnreadableRecord_andHandleTheNext() {
        when(erasure.erase(USER, DELETED_AT)).thenReturn(Mono.empty());

        StepVerifier.create(consumer.userDeleted().apply(Flux.just(record("not json"), deletionOf(USER))))
                .expectComplete().verify(Duration.ofSeconds(10));

        verify(erasure, times(1)).erase(anyString(), any());

        assertThat(technicalErrors(GlobalErrorHandler.UNEXPECTED_FAILURE_CODE, GlobalErrorHandler.SOURCE_KAFKA)).isEqualTo(1);
    }

    @Test
    void userDeleted_shouldSkipARecordWithoutUserId() {
        StepVerifier.create(consumer.userDeleted().apply(Flux.just(record("{\"deletedAt\":\"2026-10-15T14:03:27Z\"}"),
                        record("{\"userId\":\"  \"}"))))
                .expectComplete().verify(Duration.ofSeconds(10));

        verifyNoInteractions(erasure);
    }

    @Test
    void userDeleted_shouldRetryAFailedErasure_thenSucceed() {
        AtomicInteger attempts = new AtomicInteger();
        when(erasure.erase(USER, DELETED_AT)).thenReturn(Mono.defer(() -> attempts.incrementAndGet() < 3
                ? Mono.error(new TechnicalException(TechnicalErrorCode.STORE_UNAVAILABLE))
                : Mono.empty()));

        StepVerifier.withVirtualTime(() -> consumer.userDeleted().apply(Flux.just(deletionOf(USER))))
                .thenAwait(Duration.ofMinutes(5))
                .verifyComplete();

        assertThat(attempts.get()).isEqualTo(3);
    }

    @Test
    @DisplayName("an erasure that keeps failing is logged and left to reconciliation; the stream goes on")
    void userDeleted_shouldLogAndContinue_whenErasureFailsAfterEveryRetry() {
        AtomicInteger attempts = new AtomicInteger();
        when(erasure.erase(USER, DELETED_AT)).thenReturn(Mono.defer(() -> {
            attempts.incrementAndGet();
            return Mono.error(new TechnicalException(TechnicalErrorCode.STORE_UNAVAILABLE));
        }));
        when(erasure.erase(OTHER, DELETED_AT)).thenReturn(Mono.empty());

        StepVerifier.withVirtualTime(() -> consumer.userDeleted().apply(Flux.just(deletionOf(USER), deletionOf(OTHER))))
                .thenAwait(Duration.ofMinutes(10))
                .verifyComplete();

        assertThat(attempts.get()).isEqualTo(UserDeletedConsumer.MAX_ATTEMPTS + 1);
        verify(erasure).erase(OTHER, DELETED_AT);
        // The WARN line names the last failure, not "Retries exhausted".
        assertThat(logs.list).extracting(ILoggingEvent::getFormattedMessage)
                .singleElement().asString()
                .contains(USER, "CHT-TEC-004")
                .doesNotContain("Retries exhausted");

        assertThat(technicalErrors("CHT-TEC-004", GlobalErrorHandler.SOURCE_KAFKA)).isEqualTo(1);
    }

    @Test
    void userDeleted_shouldLogTheBusinessCode_whenErasureIsRefused() {
        when(erasure.erase(USER, DELETED_AT)).thenReturn(Mono.error(new BusinessException(BusinessErrorCode.ACCOUNT_DELETED)));

        StepVerifier.withVirtualTime(() -> consumer.userDeleted().apply(Flux.just(deletionOf(USER))))
                .thenAwait(Duration.ofMinutes(10))
                .verifyComplete();

        assertThat(logs.list).extracting(ILoggingEvent::getFormattedMessage)
                .singleElement().asString()
                .contains(USER, "CHT-BUS-011");

        // A refused erasure is not an incident: logged, never counted.
        assertThat(meterRegistry.getMeters()).isEmpty();
    }

    @Test
    void userDeleted_shouldLogTheFailuresTypeAndMessage_whenItIsNotADomainError() {
        when(erasure.erase(USER, DELETED_AT)).thenReturn(Mono.error(new IllegalStateException("relay down")));

        StepVerifier.withVirtualTime(() -> consumer.userDeleted().apply(Flux.just(deletionOf(USER))))
                .thenAwait(Duration.ofMinutes(10))
                .verifyComplete();

        assertThat(logs.list).extracting(ILoggingEvent::getFormattedMessage)
                .singleElement().asString()
                .contains("java.lang.IllegalStateException: relay down");

        assertThat(technicalErrors(GlobalErrorHandler.UNEXPECTED_FAILURE_CODE, GlobalErrorHandler.SOURCE_KAFKA)).isEqualTo(1);
    }

    @Test
    @DisplayName("nothing runs until the binder subscribes: the function never subscribes itself")
    void userDeleted_shouldDoNothingUntilTheFrameworkSubscribes() {
        consumer.userDeleted().apply(Flux.just(deletionOf(USER)));

        verify(erasure, never()).erase(anyString(), any());
    }
}
