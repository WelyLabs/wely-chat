package com.calendar.chat.infrastructure.presence;

import com.calendar.chat.domain.services.LiveSessions;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

/** Periodically completes this pod's streams of deleted accounts, since USER_DELETED reaches only one pod. */
@Slf4j
@Component
public class LiveSessionSweeper implements SmartLifecycle {

    private final LiveSessions liveSessions;
    private volatile Disposable loop;

    public LiveSessionSweeper(LiveSessions liveSessions) {
        this.liveSessions = liveSessions;
    }

    @Override
    public void start() {
        // Errors are caught inside the repeated Mono, or the first failure would stop the loop.
        loop = Mono.defer(liveSessions::closeDeleted)
                .onErrorResume(error -> {
                    log.warn("Stream sweep failed; next attempt in {}", LiveSessions.SWEEP_PERIOD, error);
                    return Mono.empty();
                })
                .repeatWhen(sweeps -> sweeps.delayElements(LiveSessions.SWEEP_PERIOD))
                .subscribe(
                        unused -> {
                        },
                        error -> log.error("Stream sweep loop terminated and will not restart", error));
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
