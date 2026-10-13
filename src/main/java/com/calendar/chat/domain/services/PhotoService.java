package com.calendar.chat.domain.services;

import com.calendar.chat.domain.models.ConversationDetail;
import com.calendar.chat.domain.models.ConversationPhoto;
import com.calendar.chat.domain.models.MediaProfile;
import com.calendar.chat.domain.models.Message;
import com.calendar.chat.domain.models.MessageBucket;
import com.calendar.chat.domain.models.Photo;
import com.calendar.chat.domain.ports.ChatRepository;
import com.calendar.chat.domain.ports.ConversationPhotoRepository;
import com.calendar.chat.domain.ports.MediaStore;
import com.calendar.chat.exception.ChatErrorCode;
import com.calendar.chat.exception.ChatException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Photos in conversations, Messenger-style: sent as a message, listed in the conversation's media
 * tab, readable only by its members through short-lived signed URLs.
 *
 * <p>Who may see a photo is who is in the conversation now: someone who joins an event's group
 * sees the photos sent before, someone who leaves it stops seeing them. The sender removes their
 * photo; in an event's group the organizer can too.
 *
 * <p>The files themselves live in wely-media, which prepares, stores and signs them.
 */
public class PhotoService {

    public static final int MAX_PAGE = 60;
    /** wely-media signs at most this many keys per call. */
    static final int MAX_URLS_PER_CALL = 200;
    static final String KEY_PREFIX = "chat/";

    private static final System.Logger LOG = System.getLogger(PhotoService.class.getName());

    private final ChatRepository chatRepository;
    private final ConversationPhotoRepository photoRepository;
    private final MediaStore mediaStore;
    private final ChatService chatService;
    private final Clock clock;

    public PhotoService(ChatRepository chatRepository, ConversationPhotoRepository photoRepository, MediaStore mediaStore,
                        ChatService chatService, Clock clock) {
        this.chatRepository = chatRepository;
        this.photoRepository = photoRepository;
        this.mediaStore = mediaStore;
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
                // Deferred: built eagerly, an outsider's upload would be sent to wely-media before membership was checked.
                .then(Mono.defer(() -> {
                    String messageId = UUID.randomUUID().toString();
                    String key = KEY_PREFIX + conversationId + "/" + messageId + ".jpg";
                    return mediaStore.store(key, upload, MediaProfile.PHOTO)
                            .flatMap(stored -> chatService.sendMessage(new Message(messageId, senderId, senderName, null,
                                            conversationId, caption == null ? "" : caption.strip(), clock.instant(),
                                            Photo.stored(stored.key(), stored.width(), stored.height())))
                                    .onErrorResume(error -> mediaStore.delete(key)
                                            .onErrorResume(cleanup -> Mono.empty())
                                            .then(Mono.error(error))))
                            .flatMap(sent -> photoRepository.save(new ConversationPhoto(conversationId, messageId,
                                            senderId, sent.photo(), sent.timestamp()))
                                    .onErrorResume(indexFailed -> Mono.empty())
                                    .thenReturn(sent));
                }))
                .flatMap(this::signed);
    }

    /** The media tab: newest first, a page at a time, for members only. */
    public Flux<ConversationPhoto> page(String conversationId, String requesterId, Instant before, int limit) {
        return requireMember(conversationId, requesterId)
                .thenMany(Flux.defer(() -> photoRepository.findPage(conversationId, before,
                        Math.clamp(limit, 1, MAX_PAGE))))
                .collectList()
                .flatMap(photos -> urlsFor(photos.stream().map(ConversationPhoto::photo).toList())
                        .map(urls -> photos.stream()
                                .map(photo -> new ConversationPhoto(photo.conversationId(), photo.messageId(),
                                        photo.senderId(), withUrl(photo.photo(), urls), photo.sentAt()))
                                .toList()))
                .flatMapIterable(photos -> photos);
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
                        .then(Mono.defer(() -> mediaStore.delete(photo.photo().key()).onErrorResume(error -> Mono.empty()))));
    }

    /** The message as a client may see it: a stored photo gets a URL that works for a while. */
    public Mono<Message> signed(Message message) {
        return signedAll(List.of(message)).map(List::getFirst);
    }

    public Mono<MessageBucket> signed(MessageBucket bucket) {
        return signedAll(bucket.messages())
                .map(messages -> new MessageBucket(bucket.conversationId(), bucket.bucketIndex(), messages))
                .defaultIfEmpty(bucket);
    }

    public Mono<ConversationDetail> signed(ConversationDetail conversation) {
        return signedAll(conversation.getMessages())
                .map(messages -> {
                    conversation.setMessages(messages);
                    return conversation;
                })
                .defaultIfEmpty(conversation);
    }

    /** One signing round for the whole list; empty for a null list. */
    private Mono<List<Message>> signedAll(List<Message> messages) {
        if (messages == null) {
            return Mono.empty();
        }
        return urlsFor(messages.stream().map(Message::photo).toList())
                .map(urls -> messages.stream().map(message -> message.hasStoredPhoto()
                        ? new Message(message.id(), message.senderId(), message.senderName(), message.receiverId(),
                                message.conversationId(), message.content(), message.timestamp(),
                                withUrl(message.photo(), urls))
                        : message).toList());
    }

    /**
     * Signed URLs for the stored photos among these, by key, in calls of at most 200 keys. wely-media
     * failing costs the URLs, not the response: the photos come back without one.
     */
    private Mono<Map<String, String>> urlsFor(List<Photo> photos) {
        List<String> keys = photos.stream()
                .filter(photo -> photo != null && photo.key() != null && !photo.removed())
                .map(Photo::key)
                .distinct()
                .toList();
        if (keys.isEmpty()) {
            return Mono.just(Map.of());
        }
        return Flux.range(0, (keys.size() + MAX_URLS_PER_CALL - 1) / MAX_URLS_PER_CALL)
                .map(chunk -> keys.subList(chunk * MAX_URLS_PER_CALL, Math.min(keys.size(), (chunk + 1) * MAX_URLS_PER_CALL)))
                .concatMap(chunk -> mediaStore.urls(chunk)
                        .onErrorResume(error -> {
                            LOG.log(System.Logger.Level.WARNING, "Photos sent without URL: signing failed: {0}", error.toString());
                            return Mono.just(Map.of());
                        }))
                .collect(HashMap<String, String>::new, Map::putAll)
                .map(Collections::unmodifiableMap);
    }

    private static Photo withUrl(Photo photo, Map<String, String> urls) {
        return photo.key() == null || photo.removed()
                ? photo
                : new Photo(photo.key(), photo.width(), photo.height(), urls.get(photo.key()), false);
    }

    /** Not a member, or no such conversation: both answer as missing. */
    private Mono<ConversationDetail> requireMember(String conversationId, String userId) {
        return chatRepository.findById(conversationId, userId)
                .switchIfEmpty(Mono.error(new ChatException(ChatErrorCode.CONVERSATION_NOT_FOUND)));
    }
}
