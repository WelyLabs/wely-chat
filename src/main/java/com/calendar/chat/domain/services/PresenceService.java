package com.calendar.chat.domain.services;

import com.calendar.chat.domain.models.Presence;
import com.calendar.chat.domain.ports.ChatRepository;
import com.calendar.chat.domain.ports.PresenceRepository;
import com.calendar.chat.exception.BusinessErrorCode;
import com.calendar.chat.exception.BusinessException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Who is online: a user with at least one open chat stream, on any instance.
 *
 * <p>Each stream is recorded as a connection while it lasts and refreshed by a periodic
 * heartbeat; a user counts as online while one of their connections was refreshed within
 * {@link #ONLINE_WINDOW}. A crashed instance never closes its connections, so the window — and
 * a TTL on the records — is what eventually takes its users offline.
 *
 * <p>Presence is best-effort: failing to record it never stops a stream from opening.
 */
public class PresenceService {

    /** How often each instance refreshes its open connections. */
    public static final Duration HEARTBEAT = Duration.ofSeconds(30);
    /** Three heartbeats: one missed refresh does not flicker a user offline. */
    static final Duration ONLINE_WINDOW = HEARTBEAT.multipliedBy(3);

    private final PresenceRepository presenceRepository;
    private final ChatRepository chatRepository;
    private final AccountGuard guard;
    private final String instanceId;
    private final Clock clock;

    /** The connections open on this instance, connection id to user id, for the heartbeat. */
    private final Map<String, String> localConnections = new ConcurrentHashMap<>();

    public PresenceService(PresenceRepository presenceRepository, ChatRepository chatRepository, AccountGuard guard,
                           String instanceId, Clock clock) {
        this.presenceRepository = presenceRepository;
        this.chatRepository = chatRepository;
        this.guard = guard;
        this.instanceId = instanceId;
        this.clock = clock;
    }

    /** Wraps a user's stream so the user counts as online while it is open, however it ends. */
    public <T> Flux<T> track(String userId, Flux<T> stream) {
        return Flux.usingWhen(
                open(userId),
                connectionId -> stream,
                connectionId -> close(connectionId, userId),
                (connectionId, error) -> close(connectionId, userId),
                connectionId -> close(connectionId, userId));
    }

    /** Refreshes every connection open on this instance, except those of deleted accounts. */
    public Mono<Void> heartbeat() {
        if (localConnections.isEmpty()) {
            return Mono.empty();
        }
        Map<String, String> connections = Map.copyOf(localConnections);
        return guard.deletedAmong(connections.values())
                .flatMap(deleted -> {
                    Map<String, String> alive = connections.entrySet().stream()
                            .filter(connection -> !deleted.contains(connection.getValue()))
                            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
                    return alive.isEmpty() ? Mono.<Void>empty() : presenceRepository.refresh(alive, clock.instant());
                });
    }

    /** A user's presence, only for someone sharing a conversation with them; a deleted account reads offline. */
    public Mono<Presence> getPresence(String requesterId, String userId) {
        return chatRepository.findByParticipantIds(List.of(requesterId, userId))
                .hasElement()
                .flatMap(shared -> shared
                        ? guard.isDeleted(userId).flatMap(deleted -> deleted
                                ? Mono.just(new Presence(userId, false, null))
                                : presenceRepository.find(userId, clock.instant().minus(ONLINE_WINDOW)))
                        : Mono.error(new BusinessException(BusinessErrorCode.PRESENCE_NOT_FOUND)));
    }

    /** Number of connections open on this instance. */
    int localConnectionCount() {
        return localConnections.size();
    }

    /** Registers on subscription: a stream refused before it opens leaves no connection behind. */
    private Mono<String> open(String userId) {
        return Mono.defer(() -> {
            String connectionId = UUID.randomUUID().toString();
            localConnections.put(connectionId, userId);
            return presenceRepository.connect(connectionId, userId, instanceId, clock.instant())
                    // Best-effort: the chat must open even when presence cannot be written.
                    .onErrorResume(error -> Mono.empty())
                    .thenReturn(connectionId);
        });
    }

    /** A deleted account's presence is forgotten rather than recorded as last seen. */
    private Mono<Void> close(String connectionId, String userId) {
        localConnections.remove(connectionId);
        return guard.isDeleted(userId)
                .flatMap(deleted -> deleted
                        ? presenceRepository.forget(userId)
                        : presenceRepository.disconnect(connectionId, userId, clock.instant()))
                .onErrorResume(error -> Mono.empty());
    }
}
