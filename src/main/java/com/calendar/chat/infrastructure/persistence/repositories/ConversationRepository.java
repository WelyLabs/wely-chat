package com.calendar.chat.infrastructure.persistence.repositories;

import com.calendar.chat.infrastructure.persistence.models.entities.ConversationEntity;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.data.mongodb.repository.Update;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

@Repository
public interface ConversationRepository extends ReactiveMongoRepository<ConversationEntity, String> {

    /**
     * The private conversation between these participants. Event groups are excluded: {@code $all}
     * also matches a group the two happen to share, which would open it in place of their
     * conversation and count as "sharing a conversation" for presence.
     */
    @Query("{ 'participantIds': { $all: ?0 }, 'eventId': null }")
    Mono<ConversationEntity> findByParticipantIds(List<String> participantIds);

    /** Every private conversation of the user, and the event groups someone has written in. */
    @Query("{ 'participantIds': ?0, $or: [ { 'eventId': null }, { 'lastMessage': { $ne: null } } ] }")
    Flux<ConversationEntity> findListedByParticipant(String userId);

    /**
     * Looks up a conversation the caller actually takes part in.
     *
     * <p>The participation check is part of the query rather than a test applied
     * after loading: a non-participant gets an empty result and the document is
     * never read. Every access path that takes a conversation id from the client
     * must go through this method.
     */
    Mono<ConversationEntity> findByIdAndParticipantIdsContaining(String id, String userId);

    /** An event's group, for the application itself: no member to scope it by. Never reached with a client's input. */
    Mono<ConversationEntity> findByEventId(String eventId);

    /** An event's group, scoped to its members like {@link #findByIdAndParticipantIdsContaining}. */
    Mono<ConversationEntity> findByEventIdAndParticipantIdsContaining(String eventId, String userId);

    /**
     * Replaces an event group's members and title, if what is stored is older. Matches nothing when
     * the group does not exist yet — or holds this version or a newer one.
     */
    @Query("{ 'eventId': ?0, $or: [ { 'groupVersion': { $lt: ?1 } }, { 'groupVersion': null } ] }")
    @Update("{ '$set': { 'groupVersion': ?1, 'participantIds': ?2, 'title': ?3, 'organizerId': ?4 } }")
    Mono<Long> updateEventGroup(String eventId, long version, List<String> memberIds, String title, String organizerId);

    /** The preview of a conversation whose last message is the one whose photo was removed. */
    @Query("{ '_id': ?0, 'lastMessage.m_id': ?1 }")
    @Update("{ '$set': { 'lastMessage.ph.rm': true, 'lastMessage.ph.w': 0, 'lastMessage.ph.h': 0 }, '$unset': { 'lastMessage.ph.k': '' } }")
    Mono<Long> markLastPhotoRemoved(String conversationId, String messageId);
}
