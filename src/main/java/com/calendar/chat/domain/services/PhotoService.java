package com.calendar.chat.domain.services;

import com.calendar.chat.domain.models.ConversationDetail;
import com.calendar.chat.domain.models.ConversationPhoto;
import com.calendar.chat.domain.models.Message;
import com.calendar.chat.domain.models.MessageBucket;
import com.calendar.chat.domain.models.Photo;
import com.calendar.chat.domain.ports.ChatRepository;
import com.calendar.chat.domain.ports.ConversationPhotoRepository;
import com.calendar.chat.domain.ports.PhotoNormalizer;
import com.calendar.chat.domain.ports.PhotoStorage;
import com.calendar.chat.exception.ChatErrorCode;
import com.calendar.chat.exception.ChatException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Photos in conversations, Messenger-style: sent as a message, listed in the conversation's media
 * tab, readable only by its members through short-lived signed URLs.
 *
 * <p>Who may see a photo is who is in the conversation now: someone who joins an event's group
 * sees the photos sent before, someone who leaves it stops seeing them. The sender removes their
 * photo; in an event's group the organizer can too.
 */
public class PhotoService {

    /** Long enough to scroll a conversation, short enough that a shared link stops working. */
    public static final Duration URL_TTL = Duration.ofHours(1);
    public static final int MAX_PAGE = 60;
    static final String KEY_PREFIX = "chat/";

    private final ChatRepository chatRepository;
    private final ConversationPhotoRepository photoRepository;
    private final PhotoStorage storage;
    private final PhotoNormalizer normalizer;
    private final ChatService chatService;
    private final Clock clock;

    public PhotoService(ChatRepository chatRepository, ConversationPhotoRepository photoRepository, PhotoStorage storage,
                        PhotoNormalizer normalizer, ChatService chatService, Clock clock) {
        this.chatRepository = chatRepository;
        this.photoRepository = photoRepository;
        this.storage = storage;
        this.normalizer = normalizer;
        this.chatService = chatService;
        this.clock = clock;
    }

    /**
     * Sends a photo, with an optional caption, as a message of the conversation.
     *
     * <p>Stored before the message is written, so no message ever points at a missing file; if the
     * message cannot be written, the file is removed again. The media tab's entry comes last and
     * does not fail the send: without it the photo is still in the thread.
     */
    public Mono<Message> send(String conversationId, String senderId, String senderName, byte[] upload, String caption) {
        return requireMember(conversationId, senderId)
                // Deferred: built eagerly, an outsider's upload would be decoded before membership was checked.
                .then(Mono.defer(() -> normalizer.normalize(upload)))
                .flatMap(photo -> {
                    String messageId = UUID.randomUUID().toString();
                    String key = KEY_PREFIX + conversationId + "/" + messageId + ".jpg";
                    Message message = new Message(messageId, senderId, senderName, null, conversationId,
                            caption == null ? "" : caption.strip(), clock.instant(),
                            Photo.stored(key, photo.width(), photo.height()));
                    return storage.put(key, photo.content(), photo.contentType())
                            .then(Mono.defer(() -> chatService.sendMessage(message)
                                    .onErrorResume(error -> storage.delete(key)
                                            .onErrorResume(cleanup -> Mono.empty())
                                            .then(Mono.error(error)))))
                            .flatMap(sent -> photoRepository.save(new ConversationPhoto(conversationId, messageId,
                                            senderId, sent.photo(), sent.timestamp()))
                                    .onErrorResume(indexFailed -> Mono.empty())
                                    .thenReturn(sent));
                })
                .map(this::signed);
    }

    /** The media tab: newest first, a page at a time, for members only. */
    public Flux<ConversationPhoto> page(String conversationId, String requesterId, Instant before, int limit) {
        return requireMember(conversationId, requesterId)
                .thenMany(Flux.defer(() -> photoRepository.findPage(conversationId, before,
                        Math.clamp(limit, 1, MAX_PAGE))))
                .map(photo -> new ConversationPhoto(photo.conversationId(), photo.messageId(), photo.senderId(),
                        signedPhoto(photo.photo()), photo.sentAt()));
    }

    /**
     * Removes a photo for everyone. The message stays, marked as having held a photo, so the thread
     * keeps its shape; the file goes last, so a failure there leaves an orphan file rather than a
     * message pointing at nothing.
     */
    public Mono<Void> remove(String conversationId, String messageId, String requesterId) {
        return requireMember(conversationId, requesterId)
                .flatMap(conversation -> photoRepository.find(conversationId, messageId)
                        .switchIfEmpty(Mono.error(new ChatException(ChatErrorCode.PHOTO_NOT_FOUND)))
                        .flatMap(photo -> conversation.mayRemovePhotoOf(photo.senderId(), requesterId)
                                ? Mono.just(photo)
                                : Mono.error(new ChatException(ChatErrorCode.PHOTO_REMOVAL_DENIED))))
                .flatMap(photo -> chatRepository.markPhotoRemoved(conversationId, messageId)
                        .then(Mono.defer(() -> photoRepository.delete(conversationId, messageId)))
                        .then(Mono.defer(() -> storage.delete(photo.photo().key()).onErrorResume(error -> Mono.empty()))));
    }

    /** The message as a client may see it: a stored photo gets a URL that works for a while. */
    public Message signed(Message message) {
        return message.hasStoredPhoto()
                ? new Message(message.id(), message.senderId(), message.senderName(), message.receiverId(),
                        message.conversationId(), message.content(), message.timestamp(), signedPhoto(message.photo()))
                : message;
    }

    public MessageBucket signed(MessageBucket bucket) {
        return new MessageBucket(bucket.conversationId(), bucket.bucketIndex(), signedAll(bucket.messages()));
    }

    public ConversationDetail signed(ConversationDetail conversation) {
        if (conversation.getMessages() != null) {
            conversation.setMessages(signedAll(conversation.getMessages()));
        }
        return conversation;
    }

    private Photo signedPhoto(Photo photo) {
        return new Photo(photo.key(), photo.width(), photo.height(), storage.signedUrl(photo.key(), URL_TTL), false);
    }

    private List<Message> signedAll(List<Message> messages) {
        return messages == null ? null : messages.stream().map(this::signed).toList();
    }

    /** Not a member, or no such conversation: both answer as missing. */
    private Mono<ConversationDetail> requireMember(String conversationId, String userId) {
        return chatRepository.findById(conversationId, userId)
                .switchIfEmpty(Mono.error(new ChatException(ChatErrorCode.CONVERSATION_NOT_FOUND)));
    }
}
