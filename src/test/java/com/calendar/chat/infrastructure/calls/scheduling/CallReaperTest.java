package com.calendar.chat.infrastructure.calls.scheduling;

import com.calendar.chat.domain.services.CallService;
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

    private final CallService callService = mock(CallService.class);
    private final CallReaper reaper = new CallReaper(callService);

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
}
