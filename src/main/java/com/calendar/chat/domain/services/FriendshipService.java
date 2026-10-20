package com.calendar.chat.domain.services;

import com.calendar.chat.domain.models.Friendship;
import com.calendar.chat.domain.ports.FriendshipDirectory;
import com.calendar.chat.exception.BusinessErrorCode;
import com.calendar.chat.exception.BusinessException;
import reactor.core.publisher.Mono;

import java.util.List;

/** Keeps the friendships projection and answers whether two users may talk privately. */
public class FriendshipService {

    private final FriendshipDirectory directory;
    private final AccountGuard guard;

    public FriendshipService(FriendshipDirectory directory, AccountGuard guard) {
        this.directory = directory;
        this.guard = guard;
    }

    /** Applies a FRIENDSHIP_STATE value; false when stale or naming a deleted account. */
    public Mono<Boolean> applyState(Friendship friendship) {
        return guard.deletedAmong(List.of(friendship.userA(), friendship.userB()))
                .flatMap(deleted -> deleted.isEmpty() ? directory.saveIfNewer(friendship) : Mono.just(false));
    }

    /** Applies a FRIENDSHIP_STATE tombstone; false when the pair was not friends. */
    public Mono<Boolean> applyTombstone(String userId, String otherId) {
        return directory.end(userId, otherId);
    }

    /** Completes when the two are friends; fails with FRIENDSHIP_REQUIRED otherwise, oneself included. */
    public Mono<Void> requireFriends(String userId, String otherId) {
        if (userId == null || otherId == null || userId.equals(otherId)) {
            return Mono.error(new BusinessException(BusinessErrorCode.FRIENDSHIP_REQUIRED));
        }
        return directory.areFriends(userId, otherId)
                .flatMap(friends -> friends
                        ? Mono.<Void>empty()
                        : Mono.<Void>error(new BusinessException(BusinessErrorCode.FRIENDSHIP_REQUIRED)));
    }

    public Mono<Void> forget(String userId) {
        return directory.forget(userId);
    }
}
