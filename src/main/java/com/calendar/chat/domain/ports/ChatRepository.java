package com.calendar.chat.domain.ports;

import com.calendar.chat.domain.models.ConversationDetail;
import com.calendar.chat.domain.models.ConversationSummary;
import com.calendar.chat.domain.models.EventGroup;
import com.calendar.chat.domain.models.Message;
import com.calendar.chat.domain.models.MessageBucket;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

public interface ChatRepository {

    Mono<ConversationDetail> saveWithInitialBucket(List<String> participantIds);

    Mono<ConversationDetail> findByParticipantIds(List<String> participantIds);

    /**
     * Appends a message to its conversation and returns who takes part in it, the recipients
     * being taken from there rather than from the client. Empty, with nothing written, if the
     * sender does not take part in the target conversation.
     */
    Mono<List<String>> postMessage(Message message);

    Mono<MessageBucket> findBucketByConversationIdAndBucketIndex(String conversationId,
                                                                Integer bucketIndex,
                                                                String requesterId);

    Mono<ConversationDetail> findById(String conversationId, String requesterId);

    /**
     * The user's conversations as listed: every private one, and the event groups someone has
     * written in. An empty group of a past event is not worth a row.
     */
    Flux<ConversationSummary> findUserConversations(String userId);

    /** An event's group conversation, if the requester is one of its members. */
    Mono<ConversationDetail> findByEventId(String eventId, String requesterId);

    /**
     * Creates the event's group conversation or replaces its members and title. A list no newer
     * than the stored one is ignored, so a replayed or reordered record cannot undo a change.
     */
    Mono<Void> saveEventGroup(EventGroup group);

    /** Marks a message's photo as removed, in its bucket and in the conversation's preview. */
    Mono<Void> markPhotoRemoved(String conversationId, String messageId);
}
