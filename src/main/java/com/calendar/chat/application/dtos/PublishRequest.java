package com.calendar.chat.application.dtos;

import java.util.List;

/** Body of {@code POST /calls/{id}/publish}: the browser's offer and what each transceiver carries. */
public record PublishRequest(SessionDescriptionDTO offer, List<Track> tracks) {

    public record Track(String mid, String trackName) {}
}
