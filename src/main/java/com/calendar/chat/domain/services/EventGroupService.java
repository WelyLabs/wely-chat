package com.calendar.chat.domain.services;

import com.calendar.chat.domain.models.EventGroup;
import com.calendar.chat.domain.ports.ChatRepository;
import reactor.core.publisher.Mono;

/**
 * Keeps each event's group conversation in step with wely-events: its members while the event
 * lives, nothing once it is deleted.
 */
public class EventGroupService {

    private final ChatRepository chatRepository;
    private final PhotoService photoService;

    public EventGroupService(ChatRepository chatRepository, PhotoService photoService) {
        this.chatRepository = chatRepository;
        this.photoService = photoService;
    }

    /**
     * Applies wely-events' latest word on a group. A deleted event empties its group and removes
     * its photos; an older message than the one already applied changes nothing.
     */
    public Mono<Void> apply(EventGroup group) {
        if (!group.deleted()) {
            return chatRepository.saveEventGroup(group);
        }
        return chatRepository.closeEventGroup(group.eventId(), group.version())
                .flatMap(photoService::purgeConversation);
    }
}
