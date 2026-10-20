package com.calendar.chat.infrastructure.presence;

import com.calendar.chat.domain.services.PresenceService;
import com.calendar.chat.exception.GlobalErrorHandler;
import com.calendar.chat.exception.TechnicalErrorCounter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

/**
 * Background loop, started with the application, that refreshes this instance's open chat
 * connections every {@link PresenceService#HEARTBEAT}. Without it, a user connected for longer
 * than the online window would read as offline.
 */
@Slf4j
@Component
public class PresenceHeartbeat implements SmartLifecycle {

    private final PresenceService presenceService;
    private final TechnicalErrorCounter errors;
    private volatile Disposable loop;

    public PresenceHeartbeat(PresenceService presenceService, TechnicalErrorCounter errors) {
        this.presenceService = presenceService;
        this.errors = errors;
    }

    @Override
    public void start() {
        // Errors are caught inside the repeated Mono, or the first failure would stop the loop.
        loop = Mono.defer(presenceService::heartbeat)
                .onErrorResume(error -> {
                    errors.count(error, GlobalErrorHandler.SOURCE_JOB);
                    log.warn("Presence heartbeat failed; next attempt in {}", PresenceService.HEARTBEAT, error);
                    return Mono.empty();
                })
                .repeatWhen(beats -> beats.delayElements(PresenceService.HEARTBEAT))
                .subscribe(
                        unused -> {
                        },
                        error -> log.error("Presence heartbeat loop terminated and will not restart", error));
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
