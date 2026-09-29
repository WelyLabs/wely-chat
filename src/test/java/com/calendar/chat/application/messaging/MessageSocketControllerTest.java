package com.calendar.chat.application.messaging;

import com.calendar.chat.application.dtos.ChatInputDTO;
import com.calendar.chat.domain.models.Message;
import com.calendar.chat.domain.services.ChatService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.oauth2.jwt.Jwt;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MessageSocketControllerTest {

    private static final String SENDER_ID = "sender-1";
    private static final String SENDER_NAME = "theo";

    @Mock private ChatService chatService;
    @Mock private Jwt jwt;

    private MessageSocketController controller;

    @BeforeEach
    void setUp() {
        controller = new MessageSocketController(chatService);
    }

    @Test
    @DisplayName("l'expéditeur vient du token, pas du payload envoyé par le client")
    void sendMessage_shouldTakeTheSenderFromTheToken() {
        when(jwt.getClaimAsString("businessId")).thenReturn(SENDER_ID);
        when(jwt.getClaimAsString("preferred_username")).thenReturn(SENDER_NAME);
        when(chatService.sendMessage(any(Message.class))).thenReturn(Mono.empty());

        // Le client prétend être quelqu'un d'autre dans senderUsername : ignoré.
        ChatInputDTO input = new ChatInputDTO("attacker-claimed-name", "receiver-1", "conv-1", "hello");

        StepVerifier.create(controller.sendMessage(jwt, input)).verifyComplete();

        ArgumentCaptor<Message> sent = ArgumentCaptor.forClass(Message.class);
        verify(chatService).sendMessage(sent.capture());

        assertThat(sent.getValue().senderId()).isEqualTo(SENDER_ID);
        assertThat(sent.getValue().senderName()).isEqualTo(SENDER_NAME);
        assertThat(sent.getValue().senderName()).isNotEqualTo("attacker-claimed-name");
    }

    @Test
    void sendMessage_shouldCarryTheClientPayloadFields() {
        when(jwt.getClaimAsString("businessId")).thenReturn(SENDER_ID);
        when(jwt.getClaimAsString("preferred_username")).thenReturn(SENDER_NAME);
        when(chatService.sendMessage(any(Message.class))).thenReturn(Mono.empty());

        controller.sendMessage(jwt, new ChatInputDTO(null, "receiver-1", "conv-1", "hello")).block();

        ArgumentCaptor<Message> sent = ArgumentCaptor.forClass(Message.class);
        verify(chatService).sendMessage(sent.capture());

        Message message = sent.getValue();
        assertThat(message.receiverId()).isEqualTo("receiver-1");
        assertThat(message.conversationId()).isEqualTo("conv-1");
        assertThat(message.content()).isEqualTo("hello");
        assertThat(message.id()).isNull();          // attribué à la persistance
        assertThat(message.timestamp()).isNotNull();
    }

    @Test
    @DisplayName("le flux ne délivre que les messages destinés à l'appelant")
    void streamMessage_shouldScopeTheStreamToTheCaller() {
        when(jwt.getClaimAsString("businessId")).thenReturn(SENDER_ID);
        Message mine = new Message("m-1", "other", "Other", SENDER_ID, "conv-1", "hi", LocalDateTime.now());
        when(chatService.streamMessages(SENDER_ID)).thenReturn(Flux.just(mine));

        StepVerifier.create(controller.streamMessage(jwt))
                .expectNext(mine)
                .verifyComplete();

        verify(chatService).streamMessages(SENDER_ID);
    }
}
