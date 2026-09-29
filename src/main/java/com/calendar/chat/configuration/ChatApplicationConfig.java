package com.calendar.chat.configuration;

import com.calendar.chat.domain.ports.ChatRepository;
import com.calendar.chat.domain.services.ChatService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.repository.config.EnableReactiveMongoRepositories;

/**
 * Wires the chat domain service by hand so the domain layer carries no Spring
 * annotation and stays testable without a context.
 */
@Configuration
@EnableReactiveMongoRepositories(basePackages = "com.calendar.chat.infrastructure.persistence.repositories")
public class ChatApplicationConfig {

    @Bean
    public ChatService chatService(ChatRepository chatRepository) {
        return new ChatService(chatRepository);
    }
}
