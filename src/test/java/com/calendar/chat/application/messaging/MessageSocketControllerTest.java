package com.calendar.chat.application.messaging;

import com.calendar.chat.application.dtos.ChatInputDTO;
import com.calendar.chat.domain.models.Message;
import com.calendar.chat.domain.services.ChatService;
import com.calendar.chat.domain.services.PresenceService;
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

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MessageSocketControllerTest {

    private static final String SENDER_ID = "sender-1";
    private static final String SENDER_NAME = "theo";

    @Mock private ChatService chatService;
    @Mock private PresenceService presenceService;
    @Mock private Jwt jwt;

    private MessageSocketController controller;

    @BeforeEach
    void setUp() {
        controller = new MessageSocketController(chatService, presenceService);
    }

    @Test
    @DisplayName("the sender comes from the token, not from the client payload")
    void sendMessage_shouldTakeTheSenderFromTheToken() {
        when(jwt.getClaimAsString("businessId")).thenReturn(SENDER_ID);
        when(jwt.getClaimAsString("preferred_username")).thenReturn(SENDER_NAME);
        when(chatService.sendMessage(any(Message.class))).thenAnswer(call -> Mono.just(call.getArgument(0)));

        // The client claims someone else's name in senderUsername: ignored.
        ChatInputDTO input = new ChatInputDTO("attacker-claimed-name", "receiver-1", "conv-1", "hello");

        StepVerifier.create(controller.sendMessage(jwt, input)).expectNextCount(1).verifyComplete();

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
        when(chatService.sendMessage(any(Message.class))).thenAnswer(call -> Mono.just(call.getArgument(0)));

        controller.sendMessage(jwt, new ChatInputDTO(null, "receiver-1", "conv-1", "hello")).block();

        ArgumentCaptor<Message> sent = ArgumentCaptor.forClass(Message.class);
        verify(chatService).sendMessage(sent.capture());

        Message message = sent.getValue();
        assertThat(message.receiverId()).isEqualTo("receiver-1");
        assertThat(message.conversationId()).isEqualTo("conv-1");
        assertThat(message.content()).isEqualTo("hello");
        assertThat(message.id()).isNull();          // assigned by the persistence layer
        assertThat(message.timestamp()).isNotNull();
    }

    @Test
    @DisplayName("the stream only delivers messages addressed to the caller")
    void streamMessage_shouldScopeTheStreamToTheCaller() {
        when(jwt.getClaimAsString("businessId")).thenReturn(SENDER_ID);
        Message mine = new Message("m-1", "other", "Other", SENDER_ID, "conv-1", "hi", Instant.now());
        when(chatService.streamMessages(SENDER_ID)).thenReturn(Flux.just(mine));
        when(presenceService.track(eq(SENDER_ID), any())).thenAnswer(call -> call.getArgument(1));

        StepVerifier.create(controller.streamMessage(jwt))
                .expectNext(mine)
                .verifyComplete();

        verify(chatService).streamMessages(SENDER_ID);
    }

    @Test
    @DisplayName("the caller counts as online while their stream is open")
    void streamMessage_shouldTrackTheCallersPresence() {
        when(jwt.getClaimAsString("businessId")).thenReturn(SENDER_ID);
        Flux<Message> stream = Flux.empty();
        when(chatService.streamMessages(SENDER_ID)).thenReturn(stream);
        when(presenceService.track(SENDER_ID, stream)).thenReturn(Flux.empty());

        StepVerifier.create(controller.streamMessage(jwt)).verifyComplete();

        verify(presenceService).track(SENDER_ID, stream);
    }
}
