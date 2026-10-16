package com.calendar.chat.domain.ports;

import com.calendar.chat.domain.models.DeletedUserMarker;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Collection;
import java.util.Set;

/** The deleted-account markers, shared by every chat instance and kept permanently. */
public interface DeletedUserMarkers {

    /** Inserts the marker if absent; an existing marker keeps its first deletedAt. */
    Mono<Void> markDeleted(String userId, Instant deletedAt);

    /** Records that every erasure step has run. */
    Mono<Void> markErased(String userId, Instant erasedAt);

    /** Empty when the account carries no marker. */
    Mono<DeletedUserMarker> find(String userId);

    Mono<Boolean> isDeleted(String userId);

    /** The ids among these that carry a marker. */
    Flux<String> findDeletedAmong(Collection<String> userIds);

    /** The ids among these whose marker has an erasedAt; no query for an empty input. */
    Mono<Set<String>> erasedAmong(Collection<String> userIds);
}
