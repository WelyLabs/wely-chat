package com.calendar.chat.domain.ports;

import com.calendar.chat.domain.models.ConversationDetail;
import com.calendar.chat.domain.models.ConversationSummary;
import com.calendar.chat.domain.models.EventGroup;
import com.calendar.chat.domain.models.Message;
import com.calendar.chat.domain.models.MessageBucket;
import com.calendar.chat.domain.models.PostedMessage;
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

    /**
     * Appends an application notice to an event's group, with no sender check: the application
     * takes part in no conversation. A message with the same id already there is not written
     * again, and comes back marked duplicate. Empty if the event has no group yet.
     */
    Mono<PostedMessage> postSystemMessage(String eventId, Message message);

    /** Like {@link #postSystemMessage}, into any conversation by its id. Empty if it does not exist. */
    Mono<PostedMessage> postSystemMessageToConversation(String conversationId, Message message);

    /**
     * Empties a deleted event's group: no member, no message left. The conversation itself stays,
     * holding the version, so a late and older change cannot bring the group back. Emits the
     * conversation's id when this call emptied it; empty when it was already closed or newer.
     */
    Mono<String> closeEventGroup(String eventId, long version);

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

    /** One message of a conversation, as stored; empty if there is none with this id. */
    Mono<Message> findMessage(String conversationId, String messageId);
}
