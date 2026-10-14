package com.calendar.chat.domain.ports;

import com.calendar.chat.domain.models.CallEvent;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Carries call events to whichever instance holds the recipient's stream, like
 * {@link MessageBroadcaster} does for messages.
 */
public interface CallBroadcaster {

    Mono<Void> broadcast(CallEvent event);

    /** The call events addressed to this user. */
    Flux<CallEvent> subscribe(String userId);
}
