package com.calendar.chat.domain.models;

import java.util.List;

/**
 * A subscription as the browser sees it: tracks named by people, not by relay sessions.
 *
 * @param offer                 to answer through renegotiation; null when none is needed
 * @param requiresRenegotiation the browser must answer {@code offer} before media flows
 */
public record CallSubscription(SessionDescription offer, boolean requiresRenegotiation, List<ReceivedTrack> tracks) {

    public CallSubscription {
        tracks = List.copyOf(tracks);
    }

    /** A track received on transceiver {@code mid}, published by {@code userId}. */
    public record ReceivedTrack(String mid, String userId, String trackName) {}
}
