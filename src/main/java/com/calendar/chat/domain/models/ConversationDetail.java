package com.calendar.chat.domain.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ConversationDetail {
    private String id;
    private List<String> participantIds;
    private ConversationType type;
    private List<Message> messages;
    private Integer bucketIndex;
    /** The event's title for an event group; null for a private conversation. */
    private String title;
    /** The event an {@link ConversationType#EVENT} conversation belongs to; null otherwise. */
    private String eventId;
}