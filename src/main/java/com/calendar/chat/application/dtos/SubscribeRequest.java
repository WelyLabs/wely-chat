package com.calendar.chat.application.dtos;

import java.util.List;

/** Body of {@code POST /calls/{id}/subscribe}: tracks named by who publishes them. */
public record SubscribeRequest(List<Track> tracks) {

    /** {@code layer}: FULL, HALF or QUARTER for a camera; null for audio. */
    public record Track(String userId, String trackName, String layer) {}
}
