package com.calendar.chat.configuration;

import com.calendar.chat.domain.ports.PhotoNormalizer;
import com.calendar.chat.domain.ports.PhotoStorage;
import com.calendar.chat.domain.ports.ConversationPhotoRepository;
import com.calendar.chat.domain.services.PhotoService;
import com.calendar.chat.domain.ports.ChatRepository;
import com.calendar.chat.domain.ports.MessageBroadcaster;
import com.calendar.chat.domain.ports.PresenceRepository;
import com.calendar.chat.domain.services.PresenceService;
import com.calendar.chat.domain.services.ChatService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.repository.config.EnableReactiveMongoRepositories;

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

        ChatService service = config.chatService(repository, broadcaster);

        // No @Service on ChatService: that is what makes it testable without a Spring
        // context and keeps the domain free of framework annotations.
        assertThat(service).isNotNull();
        verifyNoInteractions(repository, broadcaster);
    }

    @Test
    @DisplayName("the photo service is built by hand too, from its ports")
    void photoService_shouldBuildTheDomainServiceFromItsPorts() {
        PhotoService service = config.photoService(mock(ChatRepository.class), mock(ConversationPhotoRepository.class),
                mock(PhotoStorage.class), mock(PhotoNormalizer.class), mock(ChatService.class));

        assertThat(service).isNotNull();
    }

    @Test
    @DisplayName("the presence service is built by hand too, from its ports")
    void presenceService_shouldBuildTheDomainServiceFromItsPorts() {
        PresenceRepository presence = mock(PresenceRepository.class);
        ChatRepository chats = mock(ChatRepository.class);

        PresenceService service = config.presenceService(presence, chats, "pod-1");

        assertThat(service).isNotNull();
        verifyNoInteractions(presence, chats);
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
