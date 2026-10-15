package com.calendar.chat.domain.models;

import java.util.List;

/**
 * Who belongs to an event's group conversation, as wely-events last said.
 *
 * @param organizerId who organizes the event; null in messages sent before it was named
 * @param memberIds the organizer and everyone taking part
 * @param version   grows with every change, so an older list never overwrites a newer one
 * @param deleted   the event is gone: its group goes with it
 */
public record EventGroup(String eventId, String title, String organizerId, List<String> memberIds, long version,
                         boolean deleted) {

    public EventGroup(String eventId, String title, String organizerId, List<String> memberIds, long version) {
        this(eventId, title, organizerId, memberIds, version, false);
    }

    /** As announced before wely-events named the organizer. */
    public EventGroup(String eventId, String title, List<String> memberIds, long version) {
        this(eventId, title, null, memberIds, version, false);
    }
}
