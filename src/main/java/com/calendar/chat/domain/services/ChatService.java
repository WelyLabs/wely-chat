package com.calendar.chat.domain.services;

import com.calendar.chat.domain.models.ConversationDetail;
import com.calendar.chat.domain.models.ConversationSummary;
import com.calendar.chat.domain.models.Message;
import com.calendar.chat.domain.models.MessageBucket;
import com.calendar.chat.domain.ports.ChatRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.List;

public class ChatService {

    private final ChatRepository chatRepository;
    private final Sinks.Many<Message> sink = Sinks.many().multicast().directBestEffort();

    public ChatService(ChatRepository chatRepository) {
        this.chatRepository = chatRepository;
    }

    public Mono<ConversationDetail> readOrCreateConversation(List<String> participantIds) {
        return chatRepository.findByParticipantIds(participantIds)
                .switchIfEmpty(Mono.defer(() -> {
                    return chatRepository.saveWithInitialBucket(participantIds);
                }));
    }

    // todo : gestion des erreurs
    public Mono<Void> sendMessage(Message message) {
        sink.tryEmitNext(message);
        return chatRepository.postMessage(message);
    }

    public Flux<Message> streamMessages(String userId) {
        return sink.asFlux()
                .filter(msg -> msg.receiverId().equals(userId));
    }

    /**
     * Reads a page of history. {@code requesterId} comes from the caller's token
     * and scopes the lookup: a conversation the caller does not take part in
     * yields an empty result.
     */
    public Mono<MessageBucket> readPreviousMessages(String conversationId, Integer bucketIndex, String requesterId) {
        return chatRepository.findBucketByConversationIdAndBucketIndex(conversationId, bucketIndex, requesterId);
    }

    public Mono<ConversationDetail> readConversationById(String conversationId, String requesterId) {
        return chatRepository.findById(conversationId, requesterId);
    }

    public Flux<ConversationSummary> readConversations(String userId) {
        return chatRepository.findUserConversations(userId);
    }
}
