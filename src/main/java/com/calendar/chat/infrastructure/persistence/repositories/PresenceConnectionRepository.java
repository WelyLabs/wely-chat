package com.calendar.chat.infrastructure.persistence.repositories;

import com.calendar.chat.infrastructure.persistence.models.entities.PresenceConnectionEntity;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.data.mongodb.repository.Update;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Collection;

@Repository
public interface PresenceConnectionRepository extends ReactiveMongoRepository<PresenceConnectionEntity, String> {

    Mono<Boolean> existsByUserIdAndSeenAtAfter(String userId, Instant aliveSince);

    /** The heartbeat: one update for every connection an instance holds. */
    @Query("{ '_id': { $in: ?0 } }")
    @Update("{ '$set': { 'seenAt': ?1 } }")
    Mono<Long> refresh(Collection<String> connectionIds, Instant at);
}
