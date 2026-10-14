package com.calendar.chat.domain.models;

import java.util.List;

/**
 * The media relay's answer to a subscription.
 *
 * @param offer                 to answer through renegotiation; null when none is needed
 * @param requiresRenegotiation the browser must answer {@code offer} before media flows
 * @param tracks                the subscribed tracks, each with its receiving mid
 */
public record RelaySubscription(SessionDescription offer, boolean requiresRenegotiation, List<RemoteTrack> tracks) {

    public RelaySubscription {
        tracks = List.copyOf(tracks);
    }
}
