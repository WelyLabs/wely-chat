package com.calendar.chat.domain.models;

import java.time.Instant;
import java.util.List;

/**
 * @param participantIds who takes part: the client needs it to name a private conversation after
 *                       the other participant, the summary having no title of its own.
 * @param title          the event's title for an event group; null for a private conversation.
 * @param eventId        the event an {@link ConversationType#EVENT} conversation belongs to, so the
 *                       client can open the event page; null otherwise.
 */
public record ConversationSummary(
    String id,
    List<String> participantIds,
    ConversationType type,
    String title,
    Instant updatedAt,
    Message lastMessage,
    String eventId
) { }
