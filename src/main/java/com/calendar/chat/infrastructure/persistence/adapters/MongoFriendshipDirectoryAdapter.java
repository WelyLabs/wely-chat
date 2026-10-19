package com.calendar.chat.infrastructure.persistence.adapters;

import com.calendar.chat.domain.models.Friendship;
import com.calendar.chat.domain.ports.FriendshipDirectory;
import com.calendar.chat.infrastructure.persistence.mappers.FriendshipPersistenceMapper;
import com.calendar.chat.infrastructure.persistence.repositories.FriendshipMongoRepository;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/** The projection in MongoDB's friendships; every failure but the expected duplicate becomes STORE_UNAVAILABLE. */
@Component
public class MongoFriendshipDirectoryAdapter implements FriendshipDirectory {

    private final FriendshipMongoRepository repository;
    private final FriendshipPersistenceMapper mapper;

    public MongoFriendshipDirectoryAdapter(FriendshipMongoRepository repository, FriendshipPersistenceMapper mapper) {
        this.repository = repository;
        this.mapper = mapper;
    }

    /** An insert first: a pair already stored hits the unique _id and is updated under the version guard. */
    @Override
    public Mono<Boolean> saveIfNewer(Friendship friendship) {
        return repository.insert(mapper.toEntity(friendship))
                .thenReturn(true)
                .onErrorResume(DuplicateKeyException.class, stored -> repository
                        .befriendIfOlder(friendship.pairId(), friendship.version())
                        .map(updated -> updated > 0))
                .onErrorMap(DataAccessException.class, StoreErrors::unavailable);
    }

    @Override
    public Mono<Boolean> end(String userId, String otherId) {
        return repository.endIfFriends(Friendship.pairIdOf(userId, otherId))
                .map(updated -> updated > 0)
                .onErrorMap(DataAccessException.class, StoreErrors::unavailable);
    }

    @Override
    public Mono<Boolean> areFriends(String userId, String otherId) {
        return repository.existsFriends(Friendship.pairIdOf(userId, otherId))
                .onErrorMap(DataAccessException.class, StoreErrors::unavailable);
    }

    @Override
    public Mono<Void> forget(String userId) {
        return repository.deleteInvolving(userId)
                .onErrorMap(DataAccessException.class, StoreErrors::unavailable)
                .then();
    }
}
