package com.calendar.chat.configuration;

import com.calendar.chat.domain.ports.MediaStore;
import com.calendar.chat.domain.ports.ConversationPhotoRepository;
import com.calendar.chat.domain.services.PhotoService;
import com.calendar.chat.domain.ports.CallBroadcaster;
import com.calendar.chat.domain.ports.CallNoticeFormatter;
import com.calendar.chat.domain.ports.CallRepository;
import com.calendar.chat.domain.ports.ChatRepository;
import com.calendar.chat.domain.ports.IceServerProvider;
import com.calendar.chat.domain.ports.MediaRelay;
import com.calendar.chat.domain.services.CallService;
import com.calendar.chat.domain.services.EventGroupService;
import com.calendar.chat.domain.services.FriendshipService;
import com.calendar.chat.domain.ports.MessageBroadcaster;
import com.calendar.chat.domain.ports.PresenceRepository;
import com.calendar.chat.domain.services.PresenceService;
import com.calendar.chat.domain.services.ChatService;
import com.calendar.chat.domain.ports.DeletedUserMarkers;
import com.calendar.chat.domain.ports.FriendshipDirectory;
import com.calendar.chat.domain.ports.DeletedUsersSource;
import com.calendar.chat.domain.services.AccountErasureService;
import com.calendar.chat.domain.services.AccountGuard;
import com.calendar.chat.domain.services.LiveSessions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.repository.config.EnableReactiveMongoRepositories;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

class ChatApplicationConfigTest {

    private final ChatApplicationConfig config = new ChatApplicationConfig();

    @Test
    @DisplayName("the domain service is built by hand, from its port")
    void chatService_shouldBuildTheDomainServiceFromItsPort() {
        ChatRepository repository = mock(ChatRepository.class);
        MessageBroadcaster broadcaster = mock(MessageBroadcaster.class);

        ChatService service = config.chatService(repository, broadcaster, mock(AccountGuard.class),
                mock(FriendshipService.class));

        // No @Service on ChatService: that is what makes it testable without a Spring
        // context and keeps the domain free of framework annotations.
        assertThat(service).isNotNull();
        verifyNoInteractions(repository, broadcaster);
    }

    @Test
    @DisplayName("the photo service is built by hand too, from its ports")
    void photoService_shouldBuildTheDomainServiceFromItsPorts() {
        PhotoService service = config.photoService(mock(ChatRepository.class), mock(ConversationPhotoRepository.class),
                mock(MediaStore.class), mock(ChatService.class), mock(AccountGuard.class));

        assertThat(service).isNotNull();
    }

    @Test
    @DisplayName("the presence service is built by hand too, from its ports")
    void presenceService_shouldBuildTheDomainServiceFromItsPorts() {
        PresenceRepository presence = mock(PresenceRepository.class);
        ChatRepository chats = mock(ChatRepository.class);

        PresenceService service = config.presenceService(presence, chats, mock(AccountGuard.class), "pod-1");

        assertThat(service).isNotNull();
        verifyNoInteractions(presence, chats);
    }

    @Test
    @DisplayName("the event group service is built by hand too")
    void eventGroupService_shouldBuildTheDomainService() {
        ChatRepository repository = mock(ChatRepository.class);

        EventGroupService service = config.eventGroupService(repository, mock(PhotoService.class));

        assertThat(service).isNotNull();
        verifyNoInteractions(repository);
    }

    @Test
    @DisplayName("the call service is built by hand too, from its ports")
    void callService_shouldBuildTheDomainServiceFromItsPorts() {
        CallRepository calls = mock(CallRepository.class);
        MediaRelay relay = mock(MediaRelay.class);

        CallService service = config.callService(calls, mock(ChatRepository.class), relay,
                mock(IceServerProvider.class), mock(CallBroadcaster.class), mock(ChatService.class),
                mock(CallNoticeFormatter.class), mock(AccountGuard.class), 12);

        assertThat(service).isNotNull();
        verifyNoInteractions(calls, relay);
    }

    @Test
    void accountGuard_shouldBuildTheDomainServiceFromItsPort() {
        DeletedUserMarkers markers = mock(DeletedUserMarkers.class);

        assertThat(config.accountGuard(markers)).isNotNull();
        verifyNoInteractions(markers);
    }

    @Test
    void friendshipService_shouldBuildTheDomainServiceFromItsPort_withoutTouchingIt() {
        FriendshipDirectory directory = mock(FriendshipDirectory.class);
        AccountGuard guard = mock(AccountGuard.class);

        FriendshipService service = config.friendshipService(directory, guard);

        assertThat(service).isNotNull();
        verifyNoInteractions(directory, guard);
    }

    @Test
    void liveSessions_shouldBuildTheDomainService() {
        AccountGuard guard = mock(AccountGuard.class);

        assertThat(config.liveSessions(guard)).isNotNull();
        verifyNoInteractions(guard);
    }

    @Test
    void accountErasureService_shouldBuildTheDomainServiceFromItsPorts_withoutTouchingThem() {
        DeletedUserMarkers markers = mock(DeletedUserMarkers.class);
        ChatRepository chats = mock(ChatRepository.class);
        PresenceRepository presence = mock(PresenceRepository.class);

        AccountErasureService service = config.accountErasureService(markers, chats, presence,
                mock(CallService.class), mock(LiveSessions.class), mock(FriendshipService.class));

        assertThat(service).isNotNull();
        verifyNoInteractions(markers, chats, presence);
    }

    @Test
    void accountReconciliationService_shouldBuildTheDomainServiceFromItsPorts_withoutTouchingThem() {
        DeletedUsersSource source = mock(DeletedUsersSource.class);
        DeletedUserMarkers markers = mock(DeletedUserMarkers.class);
        AccountErasureService erasure = mock(AccountErasureService.class);

        assertThat(config.accountReconciliationService(source, markers, erasure, Duration.ofHours(48), 500))
                .isNotNull();
        verifyNoInteractions(source, markers, erasure);
    }

    @Test
    @DisplayName("the scanned repositories are the reactive ones, not the imperative ones")
    void config_shouldEnableReactiveMongoRepositories() {
        // @EnableMongoRepositories would scan for blocking repositories and switch off
        // the reactive auto-configuration for the package.
        EnableReactiveMongoRepositories annotation =
                ChatApplicationConfig.class.getAnnotation(EnableReactiveMongoRepositories.class);

        assertThat(annotation).isNotNull();
        assertThat(annotation.basePackages())
                .containsExactly("com.calendar.chat.infrastructure.persistence.repositories");
    }
}
