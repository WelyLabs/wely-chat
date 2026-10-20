package com.calendar.chat.infrastructure.presence;

import com.calendar.chat.domain.services.PresenceService;
import com.calendar.chat.exception.GlobalErrorHandler;
import com.calendar.chat.exception.TechnicalErrorCounter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PresenceHeartbeatTest {

    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    private double technicalErrors(String code, String source) {
        var counter = meterRegistry.find(GlobalErrorHandler.TECHNICAL_ERRORS_METRIC)
                .tags(GlobalErrorHandler.CODE_TAG, code, GlobalErrorHandler.SOURCE_TAG, source).counter();
        return counter == null ? 0 : counter.count();
    }

    private final PresenceService presenceService = mock(PresenceService.class);
    private final PresenceHeartbeat heartbeat = new PresenceHeartbeat(presenceService, new TechnicalErrorCounter(meterRegistry));

    @AfterEach
    void stop() {
        heartbeat.stop();
    }

    @Test
    void start_shouldBeatAtOnce_andReportRunning() {
        AtomicInteger beats = new AtomicInteger();
        when(presenceService.heartbeat()).thenReturn(Mono.fromRunnable(beats::incrementAndGet));

        heartbeat.start();

        await().atMost(Duration.ofSeconds(5)).until(() -> beats.get() >= 1);
        assertThat(heartbeat.isRunning()).isTrue();
    }

    @Test
    void start_shouldSurviveAFailedBeat() {
        when(presenceService.heartbeat()).thenReturn(Mono.error(new IllegalStateException("mongo down")));

        heartbeat.start();

        assertThat(heartbeat.isRunning()).isTrue();
    }

    @Test
    void stop_shouldEndTheLoop() {
        when(presenceService.heartbeat()).thenReturn(Mono.empty());
        heartbeat.start();

        heartbeat.stop();

        assertThat(heartbeat.isRunning()).isFalse();
    }

    @Test
    void start_shouldCountAFailedBeat_asAJobFailure() {
        when(presenceService.heartbeat()).thenReturn(Mono.error(new IllegalStateException("mongo down")));

        heartbeat.start();

        await().atMost(Duration.ofSeconds(5))
                .until(() -> technicalErrors(GlobalErrorHandler.UNEXPECTED_FAILURE_CODE, GlobalErrorHandler.SOURCE_JOB) >= 1);
    }
}
