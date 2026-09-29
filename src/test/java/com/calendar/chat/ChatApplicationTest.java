package com.calendar.chat;

import com.calendar.chat.domain.ports.ChatRepository;
import com.calendar.chat.domain.services.ChatService;
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
}
