package com.calendar.chat.domain.models;

import java.util.List;

/**
 * Who belongs to an event's group conversation, as wely-events last said.
 *
 * @param organizerId who organizes the event; null in messages sent before it was named
 * @param memberIds the organizer and everyone taking part; empty once nobody does
 * @param version   grows with every change, so an older list never overwrites a newer one
 */
public record EventGroup(String eventId, String title, String organizerId, List<String> memberIds, long version) {

    /** As announced before wely-events named the organizer. */
    public EventGroup(String eventId, String title, List<String> memberIds, long version) {
        this(eventId, title, null, memberIds, version);
    }
}
