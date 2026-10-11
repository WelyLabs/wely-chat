package com.calendar.chat.domain.services;

import com.calendar.chat.domain.models.Presence;
import com.calendar.chat.domain.ports.ChatRepository;
import com.calendar.chat.domain.ports.PresenceRepository;
import com.calendar.chat.exception.ChatErrorCode;
import com.calendar.chat.exception.ChatException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

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
    private final String instanceId;
    private final Clock clock;

    /** The connections open on this instance, connection id to user id, for the heartbeat. */
    private final Map<String, String> localConnections = new ConcurrentHashMap<>();

    public PresenceService(PresenceRepository presenceRepository, ChatRepository chatRepository,
                           String instanceId, Clock clock) {
        this.presenceRepository = presenceRepository;
        this.chatRepository = chatRepository;
        this.instanceId = instanceId;
        this.clock = clock;
    }

    /**
     * Wraps a user's stream so the user counts as online while it is open, whether it ends by
     * completing, failing or being cancelled — closing the app cancels it.
     */
    public <T> Flux<T> track(String userId, Flux<T> stream) {
        return Flux.usingWhen(
                open(userId),
                connectionId -> stream,
                connectionId -> close(connectionId, userId),
                (connectionId, error) -> close(connectionId, userId),
                connectionId -> close(connectionId, userId));
    }

    /** Refreshes every connection open on this instance. */
    public Mono<Void> heartbeat() {
        if (localConnections.isEmpty()) {
            return Mono.empty();
        }
        return presenceRepository.refresh(Map.copyOf(localConnections), clock.instant());
    }

    /**
     * A user's presence, for someone they share a conversation with. Anyone else gets the same
     * answer as for an unknown user, so presence cannot be probed for arbitrary ids.
     */
    public Mono<Presence> getPresence(String requesterId, String userId) {
        return chatRepository.findByParticipantIds(List.of(requesterId, userId))
                .hasElement()
                .flatMap(shared -> shared
                        ? presenceRepository.find(userId, clock.instant().minus(ONLINE_WINDOW))
                        : Mono.error(new ChatException(ChatErrorCode.PRESENCE_NOT_FOUND)));
    }

    /** Number of connections open on this instance. */
    int localConnectionCount() {
        return localConnections.size();
    }

    private Mono<String> open(String userId) {
        String connectionId = UUID.randomUUID().toString();
        localConnections.put(connectionId, userId);
        return presenceRepository.connect(connectionId, userId, instanceId, clock.instant())
                // Best-effort: the chat must open even when presence cannot be written.
                .onErrorResume(error -> Mono.empty())
                .thenReturn(connectionId);
    }

    private Mono<Void> close(String connectionId, String userId) {
        localConnections.remove(connectionId);
        return presenceRepository.disconnect(connectionId, userId, clock.instant())
                .onErrorResume(error -> Mono.empty());
    }
}
