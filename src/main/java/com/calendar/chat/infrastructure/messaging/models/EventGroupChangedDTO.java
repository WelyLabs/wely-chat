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
        List<String> memberIds,
        long version
) {}
