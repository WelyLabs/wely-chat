package com.calendar.chat.application.messaging;

import com.calendar.chat.application.dtos.ChatInputDTO;
import com.calendar.chat.application.dtos.LiveMessageDTO;
import com.calendar.chat.application.mappers.LiveMessageMapper;
import com.calendar.chat.domain.models.Message;
import com.calendar.chat.domain.services.ChatService;
import com.calendar.chat.domain.services.PhotoService;
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
    private final PhotoService photoService;
    private final LiveMessageMapper liveMessageMapper;

    public MessageSocketController(ChatService chatService, PresenceService presenceService, PhotoService photoService,
                                   LiveMessageMapper liveMessageMapper) {
        this.chatService = chatService;
        this.presenceService = presenceService;
        this.photoService = photoService;
        this.liveMessageMapper = liveMessageMapper;
    }

    /** Answers with the stored message, which the client takes as delivered. */
    @MessageMapping("chat.send")
    public Mono<Message> sendMessage(@AuthenticationPrincipal Jwt jwt, ChatInputDTO chatInputDTO) {
        String userId = jwt.getClaimAsString("businessId");
        String username = jwt.getClaimAsString("preferred_username");

        Message message = new Message(null, userId, username, chatInputDTO.receiverId(), chatInputDTO.conversationId(), chatInputDTO.content(), Instant.now());
        return chatService.sendMessage(message);
    }

    /** Each frame says whether it is a NEW message or an UPDATED one, to replace by id. */
    @MessageMapping("chat.stream")
    public Flux<LiveMessageDTO> streamMessage(@AuthenticationPrincipal Jwt jwt) {
        String userId = jwt.getClaimAsString("businessId");

        // The user counts as online for as long as this stream is open.
        // A photo's URL is signed here, by the pod that delivers it: it never travels on the broker.
        return presenceService.track(userId, chatService.streamMessages(userId)
                .concatMap(photoService::signed)
                .map(liveMessageMapper::toDTO));
    }
}
