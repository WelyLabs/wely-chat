package com.calendar.chat.domain.models;

import java.util.List;

/**
 * Who belongs to an event's group conversation, as wely-events last said.
 *
 * @param memberIds the organizer and everyone taking part; empty once nobody does
 * @param version   grows with every change, so an older list never overwrites a newer one
 */
public record EventGroup(String eventId, String title, List<String> memberIds, long version) {
}
