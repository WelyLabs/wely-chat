package com.calendar.chat.infrastructure.persistence.adapters;

import com.calendar.chat.domain.models.DeletedUserMarker;
import com.calendar.chat.domain.ports.DeletedUserMarkers;
import com.calendar.chat.infrastructure.persistence.mappers.DeletedUserMarkerMapper;
import com.calendar.chat.infrastructure.persistence.models.entities.DeletedUserMarkerEntity;
import com.calendar.chat.infrastructure.persistence.repositories.DeletedUserMarkerMongoRepository;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Collection;

/** The markers in MongoDB's deleted_users; every failure but the expected duplicate becomes STORE_UNAVAILABLE. */
@Component
public class MongoDeletedUserMarkersAdapter implements DeletedUserMarkers {

    private final DeletedUserMarkerMongoRepository repository;
    private final DeletedUserMarkerMapper mapper;

    public MongoDeletedUserMarkersAdapter(DeletedUserMarkerMongoRepository repository, DeletedUserMarkerMapper mapper) {
        this.repository = repository;
        this.mapper = mapper;
    }

    /** An insert, not a save: a replay hits the unique _id and keeps the first deletedAt. */
    @Override
    public Mono<Void> markDeleted(String userId, Instant deletedAt) {
        return repository.insert(mapper.toEntity(new DeletedUserMarker(userId, deletedAt, null)))
                .onErrorResume(DuplicateKeyException.class, existing -> Mono.empty())
                .onErrorMap(DataAccessException.class, StoreErrors::unavailable)
                .then();
    }

    @Override
    public Mono<Void> markErased(String userId, Instant erasedAt) {
        return repository.markErased(userId, erasedAt)
                .onErrorMap(DataAccessException.class, StoreErrors::unavailable)
                .then();
    }

    @Override
    public Mono<DeletedUserMarker> find(String userId) {
        return repository.findById(userId)
                .map(mapper::toDomain)
                .onErrorMap(DataAccessException.class, StoreErrors::unavailable);
    }

    @Override
    public Mono<Boolean> isDeleted(String userId) {
        return repository.existsById(userId)
                .onErrorMap(DataAccessException.class, StoreErrors::unavailable);
    }

    @Override
    public Flux<String> findDeletedAmong(Collection<String> userIds) {
        return repository.findIdsIn(userIds)
                .map(DeletedUserMarkerEntity::getId)
                .onErrorMap(DataAccessException.class, StoreErrors::unavailable);
    }
}
