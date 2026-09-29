package com.calendar.chat.application.rest;

import com.calendar.chat.domain.models.ConversationDetail;
import com.calendar.chat.domain.models.ConversationSummary;
import com.calendar.chat.domain.models.MessageBucket;
import com.calendar.chat.domain.services.ChatService;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

@RestController
@RequestMapping("/conversations")
public class ConversationController {

    private final ChatService chatService;

    public ConversationController(ChatService chatService) {
        this.chatService = chatService;
    }

    @GetMapping
    public Mono<ResponseEntity<ConversationDetail>> getConversation(
            @AuthenticationPrincipal Jwt jwt,
            @RequestParam String friendId) {
        String userId = jwt.getClaimAsString("businessId");

        return chatService.readOrCreateConversation(List.of(userId, friendId)).map(ResponseEntity::ok);
    }

    @GetMapping("{conversationId}/loadMessages")
    public Mono<ResponseEntity<MessageBucket>> readPreviousMessages(
            @AuthenticationPrincipal Jwt jwt,
            @PathVariable String conversationId,
            @RequestParam Integer bucketIndex) {
        String userId = jwt.getClaimAsString("businessId");

        return chatService.readPreviousMessages(conversationId, bucketIndex, userId)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @GetMapping("{conversationId}")
    public Mono<ResponseEntity<ConversationDetail>> getConversationById(
            @AuthenticationPrincipal Jwt jwt,
            @PathVariable String conversationId) {
        String userId = jwt.getClaimAsString("businessId");

        return chatService.readConversationById(conversationId, userId)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @GetMapping("all")
    public ResponseEntity<Flux<ConversationSummary>> getConversations(
            @AuthenticationPrincipal Jwt jwt
    ) {

        String userId = jwt.getClaimAsString("businessId");
        return ResponseEntity.ok().body(chatService.readConversations(userId));
    }
}
