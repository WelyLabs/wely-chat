package com.calendar.chat.domain.services;

import com.calendar.chat.domain.models.BroadcastKind;
import com.calendar.chat.domain.models.ConversationDetail;
import com.calendar.chat.domain.models.ConversationSummary;
import com.calendar.chat.domain.models.EventGroup;
import com.calendar.chat.domain.models.LiveMessage;
import com.calendar.chat.domain.models.Message;
import com.calendar.chat.domain.models.MessageBucket;
import com.calendar.chat.domain.ports.ChatRepository;
import com.calendar.chat.domain.ports.MessageBroadcaster;
import com.calendar.chat.exception.ChatErrorCode;
import com.calendar.chat.exception.ChatException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.UUID;

public class ChatService {

    private static final System.Logger LOG = System.getLogger(ChatService.class.getName());

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
     * Persists a message, then hands a copy to every other member of the conversation.
     *
     * <p>The recipients come from the stored conversation, not from the client: the
     * {@code receiverId} it sends is ignored. Trusting it let a client push a live message to
     * anyone whose id it knew, and could only ever name one recipient, which a group cannot use.
     *
     * <p>That order matters and used to be the other way round: the message was pushed to
     * the recipient's stream first and written afterwards, so a failed write left the
     * recipient looking at a message that did not exist. Persistence is the source of
     * truth, so it goes first — a broadcast that fails costs a live update, not the
     * message.
     *
     * <p>Broadcasting from inside {@code concatMap} keeps the port uncalled until the write has
     * succeeded. Passing {@code broadcast(message)} straight to {@code then} used to need a
     * {@code defer}: it is evaluated while the chain is assembled, so the port ran even when the
     * write failed.
     *
     * <p>Returns the stored message: it is the sender's acknowledgement. This used to complete
     * empty, and rsocket-js resolves a request-response only on a payload, so every message stayed
     * "sending" on the sender's screen. A sender outside the conversation gets an error for the
     * same reason: an empty answer would leave the message pending instead of failed.
     */
    public Mono<Message> sendMessage(Message incoming) {
        // Every message gets an id here: a photo is removed by the id of the message that carries it.
        Message message = incoming.id() == null ? new Message(UUID.randomUUID().toString(), incoming.senderId(), incoming.senderName(),
                incoming.receiverId(), incoming.conversationId(), incoming.content(), incoming.timestamp(), incoming.photo()) : incoming;
        return chatRepository.postMessage(message)
                .switchIfEmpty(Mono.error(new ChatException(ChatErrorCode.CONVERSATION_NOT_FOUND)))
                .flatMapMany(Flux::fromIterable)
                .filter(memberId -> !memberId.equals(message.senderId()))
                .concatMap(recipientId -> messageBroadcaster.broadcast(
                        new LiveMessage(addressedTo(message, recipientId), BroadcastKind.NEW)))
                .then(Mono.just(message));
    }

    /**
     * Pushes a changed message to the conversation's members but {@code exceptUserId}, who made the
     * change and already knows. Members are read as {@code exceptUserId} sees them: nothing is sent
     * for a conversation they are not in. A recipient whose broadcast fails is skipped: the change
     * is already stored, and costs that member a reload.
     */
    public Mono<Void> broadcastUpdate(Message message, String exceptUserId) {
        return chatRepository.findById(message.conversationId(), exceptUserId)
                .flatMapMany(conversation -> Flux.fromIterable(conversation.getParticipantIds()))
                .filter(memberId -> !memberId.equals(exceptUserId))
                .concatMap(recipientId -> messageBroadcaster
                        .broadcast(new LiveMessage(addressedTo(message, recipientId), BroadcastKind.UPDATED))
                        .onErrorResume(error -> {
                            LOG.log(System.Logger.Level.WARNING, "Update of message {0} not pushed to {1}: {2}",
                                    message.id(), recipientId, error.toString());
                            return Mono.empty();
                        }))
                .then();
    }

    /** Stream of messages addressed to this user, new or updated, from any instance. */
    public Flux<LiveMessage> streamMessages(String userId) {
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

    /** An event's group, for one of its members; empty for anyone else, or before it exists. */
    public Mono<ConversationDetail> readEventConversation(String eventId, String requesterId) {
        return chatRepository.findByEventId(eventId, requesterId);
    }

    /** Brings an event's group in line with the members wely-events announced. */
    public Mono<Void> applyEventGroup(EventGroup group) {
        return chatRepository.saveEventGroup(group);
    }

    /** The same message for one recipient: a group message is delivered once per member. */
    private static Message addressedTo(Message message, String recipientId) {
        return new Message(message.id(), message.senderId(), message.senderName(), recipientId,
                message.conversationId(), message.content(), message.timestamp(), message.photo());
    }
}
