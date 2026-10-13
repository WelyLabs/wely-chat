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
    /** The event's organizer for an event group, as wely-events names them; null otherwise. */
    private String organizerId;

    public ConversationDetail(String id, List<String> participantIds, ConversationType type, List<Message> messages,
                              Integer bucketIndex, String title, String eventId) {
        this(id, participantIds, type, messages, bucketIndex, title, eventId, null);
    }

    /**
     * Who moderates an event group. Groups announced before the organizer was named fall back on
     * the first member, which is where wely-events has always put the organizer.
     */
    public String moderatorId() {
        if (type != ConversationType.EVENT) {
            return null;
        }
        if (organizerId != null) {
            return organizerId;
        }
        return participantIds == null || participantIds.isEmpty() ? null : participantIds.getFirst();
    }

    /** A photo is removed by whoever sent it, or in an event's group by its organizer too. */
    public boolean mayRemovePhotoOf(String senderId, String requesterId) {
        return requesterId.equals(senderId) || requesterId.equals(moderatorId());
    }
}