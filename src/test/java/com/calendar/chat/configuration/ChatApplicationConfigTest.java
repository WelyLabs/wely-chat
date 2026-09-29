package com.calendar.chat.configuration;

import com.calendar.chat.domain.ports.ChatRepository;
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
    @DisplayName("le service de domaine est instancié à la main, depuis son port")
    void chatService_shouldBuildTheDomainServiceFromItsPort() {
        ChatRepository repository = mock(ChatRepository.class);

        ChatService service = config.chatService(repository);

        // Pas de @Service sur ChatService : c'est ce qui le rend testable sans
        // contexte Spring et garde le domaine libre de toute annotation framework.
        assertThat(service).isNotNull();
        verifyNoInteractions(repository);
    }

    @Test
    @DisplayName("les repositories scannés sont les réactifs, pas les impératifs")
    void config_shouldEnableReactiveMongoRepositories() {
        // @EnableMongoRepositories scannerait des repositories bloquants et
        // désactiverait l'auto-configuration réactive du package.
        EnableReactiveMongoRepositories annotation =
                ChatApplicationConfig.class.getAnnotation(EnableReactiveMongoRepositories.class);

        assertThat(annotation).isNotNull();
        assertThat(annotation.basePackages())
                .containsExactly("com.calendar.chat.infrastructure.persistence.repositories");
    }
}
