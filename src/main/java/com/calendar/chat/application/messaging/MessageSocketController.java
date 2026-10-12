package com.calendar.chat.application.messaging;

import com.calendar.chat.application.dtos.ChatInputDTO;
import com.calendar.chat.domain.models.Message;
import com.calendar.chat.domain.services.ChatService;
import com.calendar.chat.domain.services.PresenceService;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;

@Controller
public class MessageSocketController {

    private final ChatService chatService;
    private final PresenceService presenceService;

    public MessageSocketController(ChatService chatService, PresenceService presenceService) {
        this.chatService = chatService;
        this.presenceService = presenceService;
    }

    /** Answers with the stored message, which the client takes as delivered. */
    @MessageMapping("chat.send")
    public Mono<Message> sendMessage(@AuthenticationPrincipal Jwt jwt, ChatInputDTO chatInputDTO) {
        String userId = jwt.getClaimAsString("businessId");
        String username = jwt.getClaimAsString("preferred_username");

        Message message = new Message(null, userId, username, chatInputDTO.receiverId(), chatInputDTO.conversationId(), chatInputDTO.content(), Instant.now());
        return chatService.sendMessage(message);
    }

    @MessageMapping("chat.stream")
    public Flux<Message> streamMessage(@AuthenticationPrincipal Jwt jwt) {
        String userId = jwt.getClaimAsString("businessId");

        // The user counts as online for as long as this stream is open.
        return presenceService.track(userId, chatService.streamMessages(userId));
    }
}
