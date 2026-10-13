package com.calendar.chat.domain.ports;

import com.calendar.chat.domain.models.ConversationPhoto;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;

/**
 * A conversation's photos, apart from its messages: the media tab pages through them newest first
 * without unwinding fifty-message buckets. Every lookup names the conversation.
 */
public interface ConversationPhotoRepository {

    Mono<Void> save(ConversationPhoto photo);

    /** Newest first, sent strictly before {@code before} when given. */
    Flux<ConversationPhoto> findPage(String conversationId, Instant before, int limit);

    Mono<ConversationPhoto> find(String conversationId, String messageId);

    Mono<Void> delete(String conversationId, String messageId);
}
