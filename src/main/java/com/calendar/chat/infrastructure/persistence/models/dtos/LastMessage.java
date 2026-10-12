package com.calendar.chat.infrastructure.persistence.models.dtos;

import java.time.Instant;

public record LastMessage(
    String text,
    String senderId,
    Instant timestamp,
    boolean isRead
) {}
