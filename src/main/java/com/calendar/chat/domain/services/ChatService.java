package com.calendar.chat.domain.services;

import com.calendar.chat.domain.models.ConversationDetail;
import com.calendar.chat.domain.models.ConversationSummary;
import com.calendar.chat.domain.models.Message;
import com.calendar.chat.domain.models.MessageBucket;
import com.calendar.chat.domain.ports.ChatRepository;
import com.calendar.chat.domain.ports.MessageBroadcaster;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

public class ChatService {

    private final ChatRepository chatRepository;
    private final MessageBroadcaster messageBroadcaster;

    public ChatService(ChatRepository chatRepository, MessageBroadcaster messageBroadcaster) {
        this.chatRepository = chatRepository;
        this.messageBroadcaster = messageBroadcaster;
    }

    public Mono<ConversationDetail> readOrCreateConversation(List<String> participantIds) {
        return chatRepository.findByParticipantIds(participantIds)
                .switchIfEmpty(Mono.defer(() -> chatRepository.saveWithInitialBucket(participantIds)));
    }

    /**
     * Persists a message, then hands it to the recipients.
     *
     * <p>That order matters and used to be the other way round: the message was pushed to
     * the recipient's stream first and written afterwards, so a failed write left the
     * recipient looking at a message that did not exist. Persistence is the source of
     * truth, so it goes first — a broadcast that fails costs a live update, not the
     * message.
     *
     * <p>The {@link Mono#defer} is not decoration. Passing {@code broadcast(message)}
     * straight to {@code then} evaluates it while the chain is being assembled, so the port
     * is called even when the write fails — the publisher is never subscribed, but the
     * method has already run. Same trap as an eagerly built {@code switchIfEmpty}.
     */
    public Mono<Void> sendMessage(Message message) {
        return chatRepository.postMessage(message)
                .then(Mono.defer(() -> messageBroadcaster.broadcast(message)));
    }

    /** Stream of messages addressed to this user, from any instance. */
    public Flux<Message> streamMessages(String userId) {
        return messageBroadcaster.subscribe(userId);
    }

    /**
     * Reads a page of history. {@code requesterId} comes from the caller's token and scopes
     * the lookup: a conversation the caller does not take part in yields an empty result.
     */
    public Mono<MessageBucket> readPreviousMessages(String conversationId, Integer bucketIndex,
                                                   String requesterId) {
        return chatRepository.findBucketByConversationIdAndBucketIndex(conversationId, bucketIndex,
                requesterId);
    }

    public Mono<ConversationDetail> readConversationById(String conversationId, String requesterId) {
        return chatRepository.findById(conversationId, requesterId);
    }

    public Flux<ConversationSummary> readConversations(String userId) {
        return chatRepository.findUserConversations(userId);
    }
}
