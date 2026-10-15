package com.calendar.chat.infrastructure.persistence.repositories;

import com.calendar.chat.infrastructure.persistence.models.entities.DeletedUserMarkerEntity;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.data.mongodb.repository.Update;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Collection;

@Repository
public interface DeletedUserMarkerMongoRepository extends ReactiveMongoRepository<DeletedUserMarkerEntity, String> {

    @Query("{ '_id': ?0 }")
    @Update("{ '$set': { 'erasedAt': ?1 } }")
    Mono<Long> markErased(String userId, Instant erasedAt);

    /** The markers among these ids, projected to the id. */
    @Query(value = "{ '_id': { '$in': ?0 } }", fields = "{ '_id': 1 }")
    Flux<DeletedUserMarkerEntity> findIdsIn(Collection<String> userIds);
}
