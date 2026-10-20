package com.calendar.chat.infrastructure.calls.scheduling;

import com.calendar.chat.domain.services.CallService;
import com.calendar.chat.exception.BusinessErrorCode;
import com.calendar.chat.exception.BusinessException;
import com.calendar.chat.exception.GlobalErrorHandler;
import com.calendar.chat.exception.TechnicalErrorCode;
import com.calendar.chat.exception.TechnicalErrorCounter;
import com.calendar.chat.exception.TechnicalException;
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

class CallReaperTest {

    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    private double technicalErrors(String code, String source) {
        var counter = meterRegistry.find(GlobalErrorHandler.TECHNICAL_ERRORS_METRIC)
                .tags(GlobalErrorHandler.CODE_TAG, code, GlobalErrorHandler.SOURCE_TAG, source).counter();
        return counter == null ? 0 : counter.count();
    }

    private final CallService callService = mock(CallService.class);
    private final CallReaper reaper = new CallReaper(callService, new TechnicalErrorCounter(meterRegistry));

    @AfterEach
    void stop() {
        reaper.stop();
    }

    @Test
    void start_shouldCleanUpAtOnce_andReportRunning() {
        AtomicInteger rounds = new AtomicInteger();
        when(callService.expire()).thenReturn(Mono.fromRunnable(rounds::incrementAndGet));

        reaper.start();

        await().atMost(Duration.ofSeconds(5)).until(() -> rounds.get() >= 1);
        assertThat(reaper.isRunning()).isTrue();
    }

    @Test
    void start_shouldSurviveAFailedRound() {
        when(callService.expire()).thenReturn(Mono.error(new IllegalStateException("mongo down")));

        reaper.start();

        assertThat(reaper.isRunning()).isTrue();
    }

    @Test
    void stop_shouldEndTheLoop() {
        when(callService.expire()).thenReturn(Mono.empty());
        reaper.start();

        reaper.stop();

        assertThat(reaper.isRunning()).isFalse();
    }

    @Test
    void start_shouldCountAFailedRound_asAJobFailure() {
        when(callService.expire()).thenReturn(Mono.error(new TechnicalException(TechnicalErrorCode.STORE_UNAVAILABLE)));

        reaper.start();

        await().atMost(Duration.ofSeconds(5))
                .until(() -> technicalErrors("CHT-TEC-004", GlobalErrorHandler.SOURCE_JOB) >= 1);
    }

    @Test
    void start_shouldNotCountARefusedRound() {
        AtomicInteger rounds = new AtomicInteger();
        when(callService.expire()).thenReturn(Mono.defer(() -> {
            rounds.incrementAndGet();
            return Mono.error(new BusinessException(BusinessErrorCode.CALL_NOT_FOUND));
        }));

        reaper.start();

        await().atMost(Duration.ofSeconds(5)).until(() -> rounds.get() >= 1);
        assertThat(meterRegistry.getMeters()).isEmpty();
    }
}
