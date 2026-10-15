package com.calendar.chat.domain.services;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/** The RSocket streams open on this instance, so those of a deleted account can be completed. */
public class LiveSessions {

    /** How often every instance ends the streams of accounts deleted since. */
    public static final Duration SWEEP_PERIOD = Duration.ofSeconds(30);

    private final AccountGuard guard;
    private final Map<String, Session> sessions = new ConcurrentHashMap<>();

    public LiveSessions(AccountGuard guard) {
        this.guard = guard;
    }

    /** Relays the stream until the account is deleted; a deleted account's stream is refused before it opens. */
    public <T> Flux<T> attach(String userId, Flux<T> stream) {
        return guard.requireActive(userId)
                .thenMany(Flux.defer(() -> {
                    String id = UUID.randomUUID().toString();
                    Sinks.Empty<Void> closed = Sinks.empty();
                    sessions.put(id, new Session(userId, closed));
                    return stream.takeUntilOther(closed.asMono())
                            .doFinally(signal -> sessions.remove(id));
                }));
    }

    /** Completes every stream this instance holds for the user. */
    public Mono<Void> close(String userId) {
        return Mono.fromRunnable(() -> sessions.values().stream()
                .filter(session -> session.userId().equals(userId))
                .forEach(session -> session.closed().tryEmitEmpty()));
    }

    /** Completes the streams of every account deleted since they opened; no query when none is open. */
    public Mono<Void> closeDeleted() {
        List<String> userIds = sessions.values().stream().map(Session::userId).distinct().toList();
        if (userIds.isEmpty()) {
            return Mono.empty();
        }
        return guard.deletedAmong(userIds)
                .flatMapMany(Flux::fromIterable)
                .concatMap(this::close)
                .then();
    }

    /** Number of streams open on this instance. */
    int sessionCount() {
        return sessions.size();
    }

    private record Session(String userId, Sinks.Empty<Void> closed) {
    }
}
