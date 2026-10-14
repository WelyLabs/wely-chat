package com.calendar.chat.infrastructure.calls.scheduling;

import com.calendar.chat.domain.services.CallService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * Background loop, like {@code PresenceHeartbeat}, that ends what nobody closed: calls ringing in
 * the void, participants whose browser vanished, calls left open for hours. Every instance runs
 * it; the conditional writes make sure only one acts on each call.
 */
@Slf4j
@Component
public class CallReaper implements SmartLifecycle {

    /** A third of the ring timeout: an unanswered call stops ringing within 40 s. */
    static final Duration PERIOD = Duration.ofSeconds(10);

    private final CallService callService;
    private volatile Disposable loop;

    public CallReaper(CallService callService) {
        this.callService = callService;
    }

    @Override
    public void start() {
        // Errors are caught inside the repeated Mono, or the first failure would stop the loop.
        loop = Mono.defer(callService::expire)
                .onErrorResume(error -> {
                    log.warn("Call cleanup failed; next attempt in {}", PERIOD, error);
                    return Mono.empty();
                })
                .repeatWhen(rounds -> rounds.delayElements(PERIOD))
                .subscribe(
                        unused -> {
                        },
                        error -> log.error("Call cleanup loop terminated and will not restart", error));
    }

    @Override
    public void stop() {
        Disposable running = loop;
        loop = null;
        if (running != null) {
            running.dispose();
        }
    }

    @Override
    public boolean isRunning() {
        return loop != null;
    }
}
