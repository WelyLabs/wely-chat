package com.calendar.chat.application.dtos;

import java.util.List;

/**
 * The relay's answer to a subscription. When {@code requiresRenegotiation}, the browser answers
 * {@code offer} through {@code PUT /calls/{id}/renegotiate} before media flows.
 */
public record SubscriptionResponse(SessionDescriptionDTO offer, boolean requiresRenegotiation, List<Track> tracks) {

    /** A track received on transceiver {@code mid}. */
    public record Track(String mid, String userId, String trackName) {}
}
