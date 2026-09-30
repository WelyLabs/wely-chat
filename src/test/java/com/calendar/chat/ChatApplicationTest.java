package com.calendar.chat;

import com.calendar.chat.domain.ports.ChatRepository;
import com.calendar.chat.domain.ports.MessageBroadcaster;
import com.calendar.chat.domain.services.ChatService;
import com.calendar.chat.infrastructure.messaging.adapters.KafkaMessageBroadcaster;
import com.calendar.chat.infrastructure.messaging.adapters.LocalMessageBroadcaster;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the Spring context actually starts.
 *
 * <p>The previous version of this file was commented out in full, so nothing checked
 * that the beans wire together — including the RSocket and security configuration,
 * which is the most intricate part of this service.
 */
@SpringBootTest
@ActiveProfiles("test")
class ChatApplicationTest {

    @Autowired
    private ApplicationContext context;

    @Test
    void contextLoads() {
        assertThat(context).isNotNull();
    }

    @Test
    void contextShouldExposeTheDomainServiceAndItsAdapter() {
        assertThat(context.getBean(ChatService.class)).isNotNull();
        assertThat(context.getBean(ChatRepository.class)).isNotNull();
    }

    /**
     * Guards the one fragile thing about the broadcast switch: {@code application.properties}
     * declares the Kafka binder unconditionally, and what keeps a workstation from trying to
     * reach a broker is only that {@code messageBroadcast} — the function named in
     * {@code spring.cloud.function.definition} — is a conditional bean that does not exist at
     * {@code chat.broadcast.mode=local}. Make that consumer unconditional and every test here
     * starts dialling Confluent Cloud.
     */
    @Test
    void contextShouldSelectTheInProcessBroadcasterWithoutABroker() {
        assertThat(context.getBean(MessageBroadcaster.class)).isInstanceOf(LocalMessageBroadcaster.class);
        assertThat(context.getBeansOfType(KafkaMessageBroadcaster.class)).isEmpty();
    }
}
