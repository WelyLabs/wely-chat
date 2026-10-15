package com.calendar.chat.infrastructure.messaging.models;

import java.util.List;

/**
 * A record of {@code EVENT_GROUP_CHANGED}, as wely-events publishes it: an event's whole member
 * list, not the change. Duplicated from wely-events on purpose — no code is shared between the
 * services, the topic is the contract.
 */
public record EventGroupChangedDTO(
        String eventId,
        String title,
        String organizerId,
        List<String> memberIds,
        long version,
        // True once the event is deleted; absent, and so false, in records sent before.
        boolean deleted
) {}
