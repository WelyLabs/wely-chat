package com.calendar.chat.infrastructure.reconciliation;

import com.calendar.chat.domain.services.AccountReconciliationService;
import com.calendar.chat.exception.BusinessException;
import com.calendar.chat.exception.TechnicalException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.time.Duration;

/** Background loop that reconciles with wely-users' tombstones: after the initial delay, then daily, hourly after a failure. */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "app.account-erasure.reconciliation", name = "enabled", havingValue = "true",
        matchIfMissing = true)
public class DeletedUsersReconciliationLoop implements SmartLifecycle {

    private final AccountReconciliationService reconciliation;
    private final Duration initialDelay;
    private final Duration interval;
    private final Duration retryInterval;

    private volatile Disposable loop;

    public DeletedUsersReconciliationLoop(
            AccountReconciliationService reconciliation,
            @Value("${app.account-erasure.reconciliation.initial-delay:1m}") Duration initialDelay,
            @Value("${app.account-erasure.reconciliation.interval:24h}") Duration interval,
            @Value("${app.account-erasure.reconciliation.retry-interval:1h}") Duration retryInterval) {
        this.reconciliation = reconciliation;
        this.initialDelay = initialDelay;
        this.interval = interval;
        this.retryInterval = retryInterval;
    }

    @Override
    public void start() {
        // Owned by this lifecycle and disposed on stop; passOnce always emits a delay, so only stop ends the loop.
        loop = Mono.delay(initialDelay)
                .then(Mono.defer(this::passOnce))
                .expand(next -> Mono.delay(next).then(Mono.defer(this::passOnce)))
                .subscribe(
                        unused -> {
                        },
                        error -> log.error("Deleted-users reconciliation loop terminated and will not restart", error),
                        () -> log.error("Deleted-users reconciliation loop completed and will not restart"));
        log.info("Deleted-users reconciliation started: first pass in {}, then every {}, {} after a failure",
                initialDelay, interval, retryInterval);
    }

    @Override
    public void stop() {
        Disposable running = loop;
        loop = null;
        if (running != null) {
            running.dispose();
            log.info("Deleted-users reconciliation stopped");
        }
    }

    @Override
    public boolean isRunning() {
        return loop != null;
    }

    /** One pass, cut after the interval; returns the delay before the next one. */
    Mono<Duration> passOnce() {
        return Mono.defer(reconciliation::reconcile)
                // No driver timeout bounds the erasures: a hung pass must delay the next one, not end the loop.
                .timeout(interval)
                .map(report -> {
                    log.info("Deleted-users reconciliation: {} tombstones, {} erased, {} failed",
                            report.tombstones(), report.erased(), report.failed());
                    return report.failed() > 0 ? retryInterval : interval;
                })
                .switchIfEmpty(Mono.error(() -> new IllegalStateException("Reconciliation pass completed empty")))
                .onErrorResume(error -> {
                    if (error instanceof TechnicalException technical) {
                        log.warn("Deleted-users reconciliation failed; next pass in {}: {} {}", retryInterval,
                                technical.getErrorCode().getCode(), technical.getMessage());
                    } else if (error instanceof BusinessException business) {
                        log.warn("Deleted-users reconciliation failed; next pass in {}: {} {}", retryInterval,
                                business.getErrorCode().getCode(), business.getMessage());
                    } else {
                        log.warn("Deleted-users reconciliation failed; next pass in {}", retryInterval, error);
                    }
                    return Mono.just(retryInterval);
                });
    }
}
