package com.calendar.chat.domain.ports;

import com.calendar.chat.domain.models.Presence;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Map;

/**
 * Where live connections are recorded, shared by every chat instance: presence held in one
 * instance's memory would be wrong for every user connected to the other.
 */
public interface PresenceRepository {

    /** Records an open connection; also the user's last-seen time. */
    Mono<Void> connect(String connectionId, String userId, String instanceId, Instant at);

    /** Forgets a closed connection, and records when the user was last seen. */
    Mono<Void> disconnect(String connectionId, String userId, Instant at);

    /** Marks these connections (id to user id) as still alive at {@code at}. */
    Mono<Void> refresh(Map<String, String> connections, Instant at);

    /** The user's presence: online if a connection was refreshed after {@code aliveSince}. */
    Mono<Presence> find(String userId, Instant aliveSince);

    /** Deletes every connection and the last-seen time of a user. */
    Mono<Void> forget(String userId);
}
