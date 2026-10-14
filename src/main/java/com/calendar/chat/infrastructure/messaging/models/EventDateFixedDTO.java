package com.calendar.chat.infrastructure.messaging.models;

import java.time.Instant;

/**
 * A record of {@code EVENT_DATE_FIXED}, as wely-events publishes it. Duplicated from wely-events on
 * purpose — no code is shared between the services, the topic is the contract.
 *
 * @param previousStart null for a date fixed by a poll; for a move by the organizer, where the
 *                      event was before it. Absent from records sent before moves existed.
 * @param previousEnd   for a move, the previous end, which may be null as an event's end may be
 */
public record EventDateFixedDTO(
        String eventId,
        String title,
        Instant start,
        Instant end,
        String organizerId,
        Instant previousStart,
        Instant previousEnd
) {

    public boolean isMove() {
        return previousStart != null;
    }
}
