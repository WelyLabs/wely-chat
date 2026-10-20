package com.calendar.chat.infrastructure.reconciliation;

import com.calendar.chat.domain.models.ReconciliationReport;
import com.calendar.chat.domain.services.AccountReconciliationService;
import com.calendar.chat.exception.BusinessErrorCode;
import com.calendar.chat.exception.BusinessException;
import com.calendar.chat.exception.TechnicalErrorCode;
import com.calendar.chat.exception.TechnicalException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Lifecycle cases run on real time with millisecond delays; the delay choice is checked on passOnce directly. */
class DeletedUsersReconciliationLoopTest {

    private static final Duration FAST = Duration.ofMillis(10);
    private static final Duration NEVER = Duration.ofHours(1);
    private static final Duration INTERVAL = Duration.ofHours(24);
    private static final Duration RETRY_INTERVAL = Duration.ofHours(1);
    private static final Duration QUIET = Duration.ofMillis(300);
    private static final Duration PATIENCE = Duration.ofSeconds(5);
    private static final ReconciliationReport CLEAN = new ReconciliationReport(3, 1, 0);

    private final AccountReconciliationService reconciliation = mock(AccountReconciliationService.class);
    private final AtomicInteger passes = new AtomicInteger();
    private DeletedUsersReconciliationLoop loop;

    @AfterEach
    void stop() {
        if (loop != null) {
            loop.stop();
        }
    }

    private DeletedUsersReconciliationLoop loop(Duration initialDelay, Duration interval, Duration retryInterval) {
        loop = new DeletedUsersReconciliationLoop(reconciliation, initialDelay, interval, retryInterval);
        return loop;
    }

    private Mono<ReconciliationReport> counted(Mono<ReconciliationReport> outcome) {
        return Mono.defer(() -> {
            passes.incrementAndGet();
            return outcome;
        });
    }

    @Test
    void start_shouldRunTheFirstPass_afterTheInitialDelay() {
        when(reconciliation.reconcile()).thenReturn(counted(Mono.just(CLEAN)));

        loop(FAST, NEVER, NEVER).start();

        await().atMost(PATIENCE).until(() -> passes.get() == 1);
        assertThat(loop.isRunning()).isTrue();
    }

    @Test
    void start_shouldRunNoPass_beforeTheInitialDelay() {
        when(reconciliation.reconcile()).thenReturn(counted(Mono.just(CLEAN)));

        loop(NEVER, FAST, FAST).start();

        await().during(QUIET).atMost(PATIENCE).until(() -> passes.get() == 0);
    }

    @Test
    @DisplayName("after a clean pass the next one waits the full interval")
    void start_shouldWaitTheInterval_afterACleanPass() {
        when(reconciliation.reconcile()).thenReturn(counted(Mono.just(CLEAN)));

        loop(FAST, NEVER, FAST).start();

        await().atMost(PATIENCE).until(() -> passes.get() == 1);
        await().during(QUIET).atMost(PATIENCE).until(() -> passes.get() == 1);
    }

    @Test
    @DisplayName("a failed pass is retried after the retry interval, and the loop then waits the interval")
    void start_shouldRetryAfterTheRetryInterval_whenAPassFails() {
        when(reconciliation.reconcile())
                .thenReturn(counted(Mono.error(new TechnicalException(TechnicalErrorCode.USERS_UNAVAILABLE))))
                .thenReturn(counted(Mono.just(CLEAN)));

        loop(FAST, NEVER, FAST).start();

        await().atMost(PATIENCE).until(() -> passes.get() == 2);
        await().during(QUIET).atMost(PATIENCE).until(() -> passes.get() == 2);
    }

    @Test
    void start_shouldKeepLooping_whenEveryPassFails() {
        when(reconciliation.reconcile()).thenReturn(counted(Mono.error(new IllegalStateException("boom"))));

        loop(FAST, NEVER, FAST).start();

        await().atMost(PATIENCE).until(() -> passes.get() >= 3);
        assertThat(loop.isRunning()).isTrue();
    }

    @Test
    void stop_shouldEndTheLoop() {
        when(reconciliation.reconcile()).thenReturn(counted(Mono.just(new ReconciliationReport(1, 0, 1))));
        loop(FAST, NEVER, FAST).start();
        await().atMost(PATIENCE).until(() -> passes.get() >= 1);

        loop.stop();
        int stoppedAt = passes.get();

        assertThat(loop.isRunning()).isFalse();
        await().during(QUIET).atMost(PATIENCE).until(() -> passes.get() == stoppedAt);
    }

    @Test
    void isRunning_shouldBeFalse_beforeStart() {
        assertThat(loop(FAST, NEVER, NEVER).isRunning()).isFalse();
    }

    @Test
    void passOnce_shouldScheduleTheNextPassAfterTheInterval_whenThePassIsClean() {
        when(reconciliation.reconcile()).thenReturn(Mono.just(CLEAN));

        StepVerifier.create(loop(NEVER, INTERVAL, RETRY_INTERVAL).passOnce())
                .expectNext(INTERVAL)
                .verifyComplete();
    }

    @Test
    void passOnce_shouldRetryAfterTheRetryInterval_whenThePassFails() {
        when(reconciliation.reconcile()).thenReturn(Mono.error(new TechnicalException(TechnicalErrorCode.USERS_UNAVAILABLE)));

        StepVerifier.create(loop(NEVER, INTERVAL, RETRY_INTERVAL).passOnce())
                .expectNext(RETRY_INTERVAL)
                .verifyComplete();
    }

    @Test
    void passOnce_shouldRetryAfterTheRetryInterval_whenThePassIsRefused() {
        when(reconciliation.reconcile()).thenReturn(Mono.error(new BusinessException(BusinessErrorCode.ACCOUNT_DELETED)));

        StepVerifier.create(loop(NEVER, INTERVAL, RETRY_INTERVAL).passOnce())
                .expectNext(RETRY_INTERVAL)
                .verifyComplete();
    }

    @Test
    void passOnce_shouldRetryAfterTheRetryInterval_whenSomeErasuresFailed() {
        when(reconciliation.reconcile()).thenReturn(Mono.just(new ReconciliationReport(3, 1, 1)));

        StepVerifier.create(loop(NEVER, INTERVAL, RETRY_INTERVAL).passOnce())
                .expectNext(RETRY_INTERVAL)
                .verifyComplete();
    }

    @Test
    void passOnce_shouldRetryAfterTheRetryInterval_whenTheServiceThrows() {
        when(reconciliation.reconcile()).thenThrow(new IllegalStateException("boom"));

        StepVerifier.create(loop(NEVER, INTERVAL, RETRY_INTERVAL).passOnce())
                .expectNext(RETRY_INTERVAL)
                .verifyComplete();
    }

    @Test
    void passOnce_shouldRetryAfterTheRetryInterval_whenThePassHangs() {
        when(reconciliation.reconcile()).thenReturn(Mono.never());

        StepVerifier.withVirtualTime(() -> loop(NEVER, INTERVAL, RETRY_INTERVAL).passOnce())
                .expectSubscription()
                .expectNoEvent(INTERVAL.minusSeconds(1))
                .thenAwait(Duration.ofSeconds(1))
                .expectNext(RETRY_INTERVAL)
                .expectComplete()
                .verify(PATIENCE);
    }

    @Test
    void passOnce_shouldWaitTheInterval_whenASlowPassEndsWithinTheInterval() {
        when(reconciliation.reconcile()).thenAnswer(invocation -> Mono.delay(INTERVAL.minusMinutes(1))
                .thenReturn(CLEAN));

        StepVerifier.withVirtualTime(() -> loop(NEVER, INTERVAL, RETRY_INTERVAL).passOnce())
                .expectSubscription()
                .thenAwait(INTERVAL.minusMinutes(1))
                .expectNext(INTERVAL)
                .expectComplete()
                .verify(PATIENCE);
    }

    @Test
    void passOnce_shouldRetryAfterTheRetryInterval_whenThePassCompletesEmpty() {
        when(reconciliation.reconcile()).thenReturn(Mono.empty());

        StepVerifier.create(loop(NEVER, INTERVAL, RETRY_INTERVAL).passOnce())
                .expectNext(RETRY_INTERVAL)
                .verifyComplete();
    }
}
