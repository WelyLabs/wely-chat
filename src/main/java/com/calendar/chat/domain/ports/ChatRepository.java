package com.calendar.chat.domain.ports;

import com.calendar.chat.domain.models.ConversationDetail;
import com.calendar.chat.domain.models.ConversationSummary;
import com.calendar.chat.domain.models.Message;
import com.calendar.chat.domain.models.MessageBucket;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

public interface ChatRepository {

    Mono<ConversationDetail> saveWithInitialBucket(List<String> participantIds);

    Mono<ConversationDetail> findByParticipantIds(List<String> participantIds);

    /**
     * Appends a message to its conversation. Fails with an empty result if the
     * sender does not take part in the target conversation.
     */
    Mono<Void> postMessage(Message message);

    Mono<MessageBucket> findBucketByConversationIdAndBucketIndex(String conversationId,
                                                                Integer bucketIndex,
                                                                String requesterId);

    Mono<ConversationDetail> findById(String conversationId, String requesterId);

    Flux<ConversationSummary> findUserConversations(String userId);
}
