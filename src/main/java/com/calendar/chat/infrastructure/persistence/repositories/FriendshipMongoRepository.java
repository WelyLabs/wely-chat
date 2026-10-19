package com.calendar.chat.infrastructure.persistence.repositories;

import com.calendar.chat.infrastructure.persistence.models.entities.FriendshipEntity;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.data.mongodb.repository.Update;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
public interface FriendshipMongoRepository extends ReactiveMongoRepository<FriendshipEntity, String> {

    /** Makes a stored pair friends at this version only if its stored version is lower: 0 otherwise. */
    @Query("{ '_id': ?0, 'version': { $lt: ?1 } }")
    @Update("{ '$set': { 'friends': true, 'version': ?1 } }")
    Mono<Long> befriendIfOlder(String id, long version);

    /** Ends a pair still friends: 0 when absent or already ended. */
    @Query("{ '_id': ?0, 'friends': true }")
    @Update("{ '$set': { 'friends': false } }")
    Mono<Long> endIfFriends(String id);

    @Query(value = "{ '_id': ?0, 'friends': true }", exists = true)
    Mono<Boolean> existsFriends(String id);

    @Query(value = "{ $or: [ { 'a': ?0 }, { 'b': ?0 } ] }", delete = true)
    Mono<Long> deleteInvolving(String userId);
}
