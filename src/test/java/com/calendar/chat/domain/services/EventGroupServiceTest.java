package com.calendar.chat.domain.services;

import com.calendar.chat.domain.models.EventGroup;
import com.calendar.chat.domain.ports.ChatRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.time.Duration;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EventGroupServiceTest {

    @Mock
    private ChatRepository chatRepository;
    @Mock
    private PhotoService photoService;

    private EventGroupService service;

    @BeforeEach
    void setUp() {
        service = new EventGroupService(chatRepository, photoService);
    }

    @Test
    void apply_shouldSaveTheMembers_ofALiveEvent() {
        EventGroup group = new EventGroup("event-1", "Board games", "ines", List.of("ines", "me"), 2);
        when(chatRepository.saveEventGroup(group)).thenReturn(Mono.empty());

        StepVerifier.create(service.apply(group)).expectComplete().verify(Duration.ofSeconds(10));

        verify(chatRepository).saveEventGroup(group);
        verify(chatRepository, never()).closeEventGroup(anyString(), org.mockito.ArgumentMatchers.anyLong());
    }

    @Test
    void apply_shouldCloseTheGroup_andRemoveItsPhotos_onceTheEventIsDeleted() {
        EventGroup deleted = new EventGroup("event-1", "Board games", null, List.of(), 5, true);
        when(chatRepository.closeEventGroup("event-1", 5)).thenReturn(Mono.just("conv-9"));
        when(photoService.purgeConversation("conv-9")).thenReturn(Mono.empty());

        StepVerifier.create(service.apply(deleted)).expectComplete().verify(Duration.ofSeconds(10));

        verify(photoService).purgeConversation("conv-9");
        verify(chatRepository, never()).saveEventGroup(deleted);
    }

    @Test
    void apply_shouldRemoveNoPhoto_whenTheGroupWasAlreadyClosed() {
        EventGroup deleted = new EventGroup("event-1", "Board games", null, List.of(), 5, true);
        when(chatRepository.closeEventGroup("event-1", 5)).thenReturn(Mono.empty());

        StepVerifier.create(service.apply(deleted)).expectComplete().verify(Duration.ofSeconds(10));

        verify(photoService, never()).purgeConversation(anyString());
    }
}
