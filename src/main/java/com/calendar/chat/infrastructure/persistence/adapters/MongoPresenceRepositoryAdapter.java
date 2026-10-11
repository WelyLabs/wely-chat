package com.calendar.chat.infrastructure.persistence.adapters;

import com.calendar.chat.domain.models.Presence;
import com.calendar.chat.domain.ports.PresenceRepository;
import com.calendar.chat.infrastructure.persistence.models.entities.LastSeenEntity;
import com.calendar.chat.infrastructure.persistence.models.entities.PresenceConnectionEntity;
import com.calendar.chat.infrastructure.persistence.repositories.LastSeenRepository;
import com.calendar.chat.infrastructure.persistence.repositories.PresenceConnectionRepository;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/** Presence in MongoDB, where every chat instance reads and writes the same records. */
@Component
public class MongoPresenceRepositoryAdapter implements PresenceRepository {

    private final PresenceConnectionRepository connectionRepository;
    private final LastSeenRepository lastSeenRepository;

    public MongoPresenceRepositoryAdapter(PresenceConnectionRepository connectionRepository,
                                          LastSeenRepository lastSeenRepository) {
        this.connectionRepository = connectionRepository;
        this.lastSeenRepository = lastSeenRepository;
    }

    @Override
    public Mono<Void> connect(String connectionId, String userId, String instanceId, Instant at) {
        return connectionRepository.save(new PresenceConnectionEntity(connectionId, userId, instanceId, at))
                .then(lastSeenRepository.save(new LastSeenEntity(userId, at)))
                .then();
    }

    @Override
    public Mono<Void> disconnect(String connectionId, String userId, Instant at) {
        return connectionRepository.deleteById(connectionId)
                .then(lastSeenRepository.save(new LastSeenEntity(userId, at)))
                .then();
    }

    @Override
    public Mono<Void> refresh(Map<String, String> connections, Instant at) {
        return connectionRepository.refresh(connections.keySet(), at)
                .thenMany(lastSeenRepository.saveAll(connections.values().stream().distinct()
                        .map(userId -> new LastSeenEntity(userId, at))
                        .toList()))
                .then();
    }

    @Override
    public Mono<Presence> find(String userId, Instant aliveSince) {
        return Mono.zip(
                        connectionRepository.existsByUserIdAndSeenAtAfter(userId, aliveSince),
                        lastSeenRepository.findById(userId).map(seen -> Optional.of(seen.getLastSeenAt()))
                                .defaultIfEmpty(Optional.empty()))
                .map(found -> new Presence(userId, found.getT1(), found.getT2().orElse(null)));
    }
}
