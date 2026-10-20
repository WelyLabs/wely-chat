package com.calendar.chat.domain.services;

import com.calendar.chat.domain.models.BroadcastKind;
import com.calendar.chat.domain.models.ConversationDetail;
import com.calendar.chat.domain.models.ConversationSummary;
import com.calendar.chat.domain.models.ConversationType;
import com.calendar.chat.domain.models.LiveMessage;
import com.calendar.chat.domain.models.Message;
import com.calendar.chat.domain.models.MessageBucket;
import com.calendar.chat.domain.models.PostedMessage;
import com.calendar.chat.domain.ports.ChatRepository;
import com.calendar.chat.domain.ports.MessageBroadcaster;
import com.calendar.chat.exception.BusinessErrorCode;
import com.calendar.chat.exception.BusinessException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.List;
import java.util.UUID;

public class ChatService {

    private static final System.Logger LOG = System.getLogger(ChatService.class.getName());

    private final ChatRepository chatRepository;
    private final MessageBroadcaster messageBroadcaster;
    private final AccountGuard guard;
    private final FriendshipService friendships;
    private final Clock clock;

    public ChatService(ChatRepository chatRepository, MessageBroadcaster messageBroadcaster, AccountGuard guard,
                       FriendshipService friendships, Clock clock) {
        this.chatRepository = chatRepository;
        this.messageBroadcaster = messageBroadcaster;
        this.guard = guard;
        this.friendships = friendships;
        this.clock = clock;
    }

    /**
     * The private conversation with a friend, created on first use, only between friends and never for or
     * with a deleted account. An existing one stays readable after the friendship ends.
     */
    public Mono<ConversationDetail> readOrCreateConversation(String requesterId, String friendId) {
        List<String> participantIds = List.of(requesterId, friendId);
        return guard.deletedAmong(participantIds)
                .flatMap(deleted -> {
                    if (deleted.contains(requesterId)) {
                        return Mono.error(new BusinessException(BusinessErrorCode.ACCOUNT_DELETED));
                    }
                    Mono<ConversationDetail> existing = chatRepository.findByParticipantIds(participantIds);
                    return deleted.contains(friendId)
                            ? existing.switchIfEmpty(Mono.error(new BusinessException(BusinessErrorCode.CONVERSATION_NOT_FOUND)))
                            : existing.switchIfEmpty(Mono.defer(() -> friendships.requireFriends(requesterId, friendId)
                                    .then(Mono.defer(() -> chatRepository.saveWithInitialBucket(participantIds)))));
                });
    }

    /** Stores a message, then pushes it to the other members; the stored message is the sender's acknowledgement. */
    public Mono<Message> sendMessage(Message incoming) {
        // Every message gets an id here: a photo is removed by the id of the message that carries it.
        Message message = incoming.id() == null ? new Message(UUID.randomUUID().toString(), incoming.senderId(), incoming.senderName(),
                incoming.receiverId(), incoming.conversationId(), incoming.content(), incoming.timestamp(), incoming.photo(),
                incoming.type()) : incoming;
        // A deleted sender, or a conversation made read-only by a deleted participant, writes nothing.
        return guard.requireActive(message.senderId())
                .then(Mono.defer(() -> chatRepository.findMembership(message.conversationId(), message.senderId())))
                .switchIfEmpty(Mono.error(new BusinessException(BusinessErrorCode.CONVERSATION_NOT_FOUND)))
                .flatMap(conversation -> conversation.isReadOnly()
                        ? Mono.<List<String>>error(new BusinessException(BusinessErrorCode.CONVERSATION_READ_ONLY))
                        : requireFriendsInDirect(conversation, message.senderId())
                                .then(Mono.defer(() -> chatRepository.postMessage(message))))
                .switchIfEmpty(Mono.error(new BusinessException(BusinessErrorCode.CONVERSATION_NOT_FOUND)))
                // Recipients come from the stored conversation, never from the client's receiverId.
                .flatMapMany(memberIds -> liveRecipients(message, memberIds, message.senderId()))
                .concatMap(recipientId -> messageBroadcaster.broadcast(
                        new LiveMessage(addressedTo(message, recipientId), BroadcastKind.NEW)))
                .then(Mono.just(message));
    }

    /** Completes for a group, or for a private conversation between friends; FRIENDSHIP_REQUIRED otherwise. */
    public Mono<Void> requireFriendsInDirect(ConversationDetail conversation, String userId) {
        if (conversation.getType() != ConversationType.DIRECT) {
            return Mono.empty();
        }
        String otherId = conversation.getParticipantIds().stream()
                .filter(id -> !id.equals(userId))
                .findFirst()
                .orElse(null);
        return friendships.requireFriends(userId, otherId);
    }

    /** Pushes a stored change to the other members of a conversation {@code exceptUserId} belongs to. */
    public Mono<Void> broadcastUpdate(Message message, String exceptUserId) {
        return chatRepository.findById(message.conversationId(), exceptUserId)
                .flatMapMany(conversation -> liveRecipients(message, conversation.getParticipantIds(), exceptUserId))
                .concatMap(recipientId -> messageBroadcaster
                        .broadcast(new LiveMessage(addressedTo(message, recipientId), BroadcastKind.UPDATED))
                        .onErrorResume(error -> {
                            LOG.log(System.Logger.Level.WARNING, "Update of message {0} not pushed to {1}: {2}",
                                    message.id(), recipientId, error.toString());
                            return Mono.empty();
                        }))
                .then();
    }

    /**
     * Posts an application notice in an event's group and pushes it to every member.
     *
     * <p>Its id is derived from the event and the text, so a redelivered record finds it stored
     * and posts nothing; the same text posted again later (a date fixed, changed, then fixed
     * back) is skipped too. Errors when the group does not exist yet, so the caller can retry
     * once wely-events has announced it. A member whose broadcast fails is skipped: the notice
     * is stored, and a retry would find it there and push nothing.
     */
    public Mono<Message> postSystemNotice(String eventId, String content) {
        Message notice = Message.system(noticeId(eventId, content), content, clock.instant());
        return chatRepository.postSystemMessage(eventId, notice)
                .switchIfEmpty(Mono.error(new BusinessException(BusinessErrorCode.CONVERSATION_NOT_FOUND)))
                .flatMap(posted -> posted.duplicate()
                        ? Mono.just(posted.message())
                        : pushToMembers(posted).thenReturn(posted.message()));
    }

    /**
     * Posts an application notice in any conversation, such as a call's summary. {@code noticeKey}
     * makes it idempotent: the same key posts once.
     */
    public Mono<Message> postConversationNotice(String conversationId, String noticeKey, String content) {
        Message notice = Message.system(noticeId(conversationId, noticeKey), content, clock.instant());
        return chatRepository.postSystemMessageToConversation(conversationId, notice)
                .switchIfEmpty(Mono.error(new BusinessException(BusinessErrorCode.CONVERSATION_NOT_FOUND)))
                .flatMap(posted -> posted.duplicate()
                        ? Mono.just(posted.message())
                        : pushToMembers(posted).thenReturn(posted.message()));
    }

    private Mono<Void> pushToMembers(PostedMessage posted) {
        return liveRecipients(posted.message(), posted.participantIds(), null)
                .concatMap(recipientId -> messageBroadcaster
                        .broadcast(new LiveMessage(addressedTo(posted.message(), recipientId), BroadcastKind.NEW))
                        .onErrorResume(error -> {
                            LOG.log(System.Logger.Level.WARNING, "Notice {0} not pushed to {1}: {2}",
                                    posted.message().id(), recipientId, error.toString());
                            return Mono.empty();
                        }))
                .then();
    }

    /** Members but {@code exceptId} and deleted accounts; unreadable markers only skip the live push. */
    private Flux<String> liveRecipients(Message message, List<String> memberIds, String exceptId) {
        List<String> others = memberIds.stream().filter(memberId -> !memberId.equals(exceptId)).toList();
        return guard.deletedAmong(others)
                .flatMapMany(deleted -> Flux.fromIterable(others).filter(memberId -> !deleted.contains(memberId)))
                .onErrorResume(error -> {
                    LOG.log(System.Logger.Level.WARNING, "Message {0} not pushed: {1}", message.id(), error.toString());
                    return Flux.empty();
                });
    }

    private static String noticeId(String eventId, String content) {
        return UUID.nameUUIDFromBytes((eventId + "\n" + content).getBytes(StandardCharsets.UTF_8)).toString();
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

    /** The same message for one recipient: a group message is delivered once per member. */
    private static Message addressedTo(Message message, String recipientId) {
        return new Message(message.id(), message.senderId(), message.senderName(), recipientId,
                message.conversationId(), message.content(), message.timestamp(), message.photo(), message.type());
    }
}
