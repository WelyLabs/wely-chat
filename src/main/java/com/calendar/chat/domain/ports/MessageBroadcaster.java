package com.calendar.chat.domain.ports;

import com.calendar.chat.domain.models.Message;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Carries a message from the instance that accepted it to every instance holding a
 * recipient's stream.
 *
 * <p>This port exists because of a specific failure. The delivery path used to be a
 * {@code Sinks.Many} held inside the domain service — a bus local to one JVM. With a single
 * replica nothing was visibly wrong; at two, a message accepted by pod A never reached a
 * recipient whose RSocket stream lived on pod B. It was lost with no error anywhere, which
 * is the worst shape a bug can take in a chat application.
 *
 * <p>The domain states what it needs — publish a message, subscribe to the ones addressed
 * to a user — and infrastructure decides how far that reaches: a local sink for a single
 * instance, a broker for several.
 */
public interface MessageBroadcaster {

    /** Hands a message to every instance, including the one calling. */
    Mono<Void> broadcast(Message message);

    /** Messages addressed to this user, whichever instance accepted them. */
    Flux<Message> subscribe(String userId);
}
