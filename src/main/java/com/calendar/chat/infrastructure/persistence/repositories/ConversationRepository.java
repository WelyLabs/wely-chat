package com.calendar.chat.infrastructure.persistence.repositories;

import com.calendar.chat.infrastructure.persistence.models.entities.ConversationEntity;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

@Repository
public interface ConversationRepository extends ReactiveMongoRepository<ConversationEntity, String> {

    @Query("{ 'participantIds': { $all: ?0 } }")
    Mono<ConversationEntity> findByParticipantIds(List<String> participantIds);

    Flux<ConversationEntity> findByParticipantIdsContaining(String userId);

    /**
     * Looks up a conversation the caller actually takes part in.
     *
     * <p>The participation check is part of the query rather than a test applied
     * after loading: a non-participant gets an empty result and the document is
     * never read. Every access path that takes a conversation id from the client
     * must go through this method.
     */
    Mono<ConversationEntity> findByIdAndParticipantIdsContaining(String id, String userId);
}
