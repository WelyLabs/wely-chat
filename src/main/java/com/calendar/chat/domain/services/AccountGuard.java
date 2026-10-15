package com.calendar.chat.domain.services;

import com.calendar.chat.domain.ports.DeletedUserMarkers;
import com.calendar.chat.exception.ChatErrorCode;
import com.calendar.chat.exception.ChatException;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

/** Refuses writes from deleted accounts and leaves them out of deliveries, from their markers. */
public class AccountGuard {

    private final DeletedUserMarkers markers;

    public AccountGuard(DeletedUserMarkers markers) {
        this.markers = markers;
    }

    /** Fails with ACCOUNT_DELETED when the account carries a marker; completes empty otherwise. */
    public Mono<Void> requireActive(String userId) {
        return markers.isDeleted(userId)
                .flatMap(deleted -> deleted
                        ? Mono.<Void>error(new ChatException(ChatErrorCode.ACCOUNT_DELETED))
                        : Mono.empty());
    }

    public Mono<Boolean> isDeleted(String userId) {
        return markers.isDeleted(userId);
    }

    /** The deleted accounts among these ids; no query for an empty list. */
    public Mono<Set<String>> deletedAmong(Collection<String> userIds) {
        if (userIds == null || userIds.isEmpty()) {
            return Mono.just(Set.of());
        }
        return markers.findDeletedAmong(userIds.stream().distinct().toList())
                .collect(Collectors.toUnmodifiableSet());
    }
}
