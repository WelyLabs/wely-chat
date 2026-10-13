package com.calendar.chat.infrastructure.messaging.models;

import java.time.Instant;

/**
 * A record of {@code EVENT_DATE_FIXED}, as wely-events publishes it. Duplicated from wely-events on
 * purpose — no code is shared between the services, the topic is the contract.
 */
public record EventDateFixedDTO(
        String eventId,
        String title,
        Instant start,
        Instant end,
        String organizerId
) {}
