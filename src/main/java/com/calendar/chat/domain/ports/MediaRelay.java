package com.calendar.chat.domain.ports;

import com.calendar.chat.domain.models.LocalTrack;
import com.calendar.chat.domain.models.RelaySubscription;
import com.calendar.chat.domain.models.RemoteTrack;
import com.calendar.chat.domain.models.SessionDescription;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * The SFU that forwards media between browsers. Each browser holds one session on it: it
 * publishes its own tracks there and receives the others' from it.
 */
public interface MediaRelay {

    /** Opens a session for one browser connection. */
    Mono<String> newSession();

    /** Publishes the tracks of {@code offer}; emits the relay's answer. */
    Mono<SessionDescription> publish(String sessionId, SessionDescription offer, List<LocalTrack> tracks);

    /** Subscribes a session to tracks published on other sessions. */
    Mono<RelaySubscription> subscribe(String sessionId, List<RemoteTrack> tracks);

    /** Completes a renegotiation the relay asked for, with the browser's answer. */
    Mono<Void> renegotiate(String sessionId, SessionDescription answer);

    /** Switches received cameras to other simulcast layers; each track carries its mid. */
    Mono<Void> updateLayers(String sessionId, List<RemoteTrack> tracks);

    /** Stops receiving the tracks on these transceivers. */
    Mono<Void> closeTracks(String sessionId, List<String> mids);
}
