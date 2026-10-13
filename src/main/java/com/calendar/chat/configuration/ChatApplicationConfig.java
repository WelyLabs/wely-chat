package com.calendar.chat.configuration;

import com.calendar.chat.domain.ports.ChatRepository;
import com.calendar.chat.domain.ports.MessageBroadcaster;
import com.calendar.chat.domain.ports.PresenceRepository;
import com.calendar.chat.domain.services.ChatService;
import com.calendar.chat.domain.services.PhotoService;
import com.calendar.chat.domain.ports.ConversationPhotoRepository;
import com.calendar.chat.domain.ports.PhotoNormalizer;
import com.calendar.chat.domain.ports.PhotoStorage;
import com.calendar.chat.domain.services.PresenceService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.repository.config.EnableReactiveMongoRepositories;

import java.time.Clock;

/**
 * Wires the chat domain service by hand so the domain layer carries no Spring
 * annotation and stays testable without a context.
 */
@Configuration
@EnableReactiveMongoRepositories(basePackages = "com.calendar.chat.infrastructure.persistence.repositories")
public class ChatApplicationConfig {

    @Bean
    public ChatService chatService(ChatRepository chatRepository,
                                  MessageBroadcaster messageBroadcaster) {
        return new ChatService(chatRepository, messageBroadcaster);
    }

    @Bean
    public PhotoService photoService(ChatRepository chatRepository, ConversationPhotoRepository photoRepository,
                                     PhotoStorage photoStorage, PhotoNormalizer photoNormalizer, ChatService chatService) {
        return new PhotoService(chatRepository, photoRepository, photoStorage, photoNormalizer, chatService,
                Clock.systemUTC());
    }

    /** The instance id is the broadcast one: each pod's own, which its connections carry. */
    @Bean
    public PresenceService presenceService(PresenceRepository presenceRepository, ChatRepository chatRepository,
                                          @Value("${chat.broadcast.instance-id}") String instanceId) {
        return new PresenceService(presenceRepository, chatRepository, instanceId, Clock.systemUTC());
    }
}
