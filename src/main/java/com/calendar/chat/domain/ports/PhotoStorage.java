package com.calendar.chat.domain.ports;

import reactor.core.publisher.Mono;

import java.time.Duration;

/** Where conversation photos are kept, privately: nothing stored is readable without a signed URL. */
public interface PhotoStorage {

    Mono<Void> put(String key, byte[] content, String contentType);

    /** Removing a missing key is not an error. */
    Mono<Void> delete(String key);

    /** A URL that reads this key until {@code ttl} has passed, computed locally without a network call. */
    String signedUrl(String key, Duration ttl);
}
