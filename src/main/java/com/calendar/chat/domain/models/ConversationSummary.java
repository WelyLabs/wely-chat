package com.calendar.chat.domain.models;

import java.time.LocalDateTime;
import java.util.List;

/**
 * @param participantIds who takes part: the client needs it to name a private conversation after
 *                       the other participant, the summary having no title of its own.
 */
public record ConversationSummary(
    String id,
    List<String> participantIds,
    ConversationType type,
    String title,
    LocalDateTime updatedAt,
    Message lastMessage
) { }