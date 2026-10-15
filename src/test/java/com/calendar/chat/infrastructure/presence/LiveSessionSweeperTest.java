package com.calendar.chat.infrastructure.presence;

import com.calendar.chat.domain.services.LiveSessions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LiveSessionSweeperTest {

    private final LiveSessions liveSessions = mock(LiveSessions.class);
    private final LiveSessionSweeper sweeper = new LiveSessionSweeper(liveSessions);

    @AfterEach
    void stop() {
        sweeper.stop();
    }

    @Test
    void start_shouldSweepAtOnce_andReportRunning() {
        AtomicInteger sweeps = new AtomicInteger();
        when(liveSessions.closeDeleted()).thenReturn(Mono.fromRunnable(sweeps::incrementAndGet));

        sweeper.start();

        await().atMost(Duration.ofSeconds(5)).until(() -> sweeps.get() >= 1);
        assertThat(sweeper.isRunning()).isTrue();
    }

    @Test
    void start_shouldSurviveAFailedSweep() {
        when(liveSessions.closeDeleted()).thenReturn(Mono.error(new IllegalStateException("mongo down")));

        sweeper.start();

        assertThat(sweeper.isRunning()).isTrue();
    }

    @Test
    void stop_shouldEndTheLoop() {
        when(liveSessions.closeDeleted()).thenReturn(Mono.empty());
        sweeper.start();

        sweeper.stop();

        assertThat(sweeper.isRunning()).isFalse();
    }
}
