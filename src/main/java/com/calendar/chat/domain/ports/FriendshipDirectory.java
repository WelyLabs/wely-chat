package com.calendar.chat.domain.ports;

import com.calendar.chat.domain.models.Friendship;
import reactor.core.publisher.Mono;

/** The friendships projection, fed by wely-social's FRIENDSHIP_STATE and read by every request path. */
public interface FriendshipDirectory {

    /** Stores the pair as friends unless a newer version is already stored; true when stored. */
    Mono<Boolean> saveIfNewer(Friendship friendship);

    /** Marks the pair as no longer friends, version kept so an older record cannot revive it; true when ended. */
    Mono<Boolean> end(String userId, String otherId);

    Mono<Boolean> areFriends(String userId, String otherId);

    /** Removes every pair the account belongs to. */
    Mono<Void> forget(String userId);
}
