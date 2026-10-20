package com.calendar.chat.domain.services;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.argThat;
import com.calendar.chat.domain.models.BroadcastKind;
import com.calendar.chat.domain.models.ConversationDetail;
import com.calendar.chat.domain.models.ConversationType;
import com.calendar.chat.domain.models.ConversationSummary;
import com.calendar.chat.domain.models.LiveMessage;
import com.calendar.chat.domain.models.Photo;
import com.calendar.chat.domain.models.Message;
import com.calendar.chat.domain.models.MessageBucket;
import com.calendar.chat.domain.models.MessageType;
import com.calendar.chat.domain.models.PostedMessage;
import com.calendar.chat.domain.ports.ChatRepository;
import com.calendar.chat.domain.ports.DeletedUserMarkers;
import com.calendar.chat.domain.ports.FriendshipDirectory;
import com.calendar.chat.domain.ports.MessageBroadcaster;
import com.calendar.chat.exception.BusinessErrorCode;
import com.calendar.chat.exception.BusinessException;
import com.calendar.chat.exception.TechnicalErrorCode;
import com.calendar.chat.exception.TechnicalException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.time.Duration;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ChatServiceTest {

    @Mock
    private ChatRepository chatRepository;

    @Mock
    private MessageBroadcaster messageBroadcaster;

    @Mock
    private DeletedUserMarkers markers;

    @Mock
    private FriendshipDirectory friendships;

    private static final Instant NOW = Instant.parse("2026-10-14T08:00:00Z");

    private ChatService chatService;

    @BeforeEach
    void setUp() {
        AccountGuard guard = new AccountGuard(markers);
        chatService = new ChatService(chatRepository, messageBroadcaster, guard, new FriendshipService(friendships, guard),
                Clock.fixed(NOW, ZoneOffset.UTC));
        // Everyone is friends unless a test says so.
        lenient().when(friendships.areFriends(anyString(), anyString())).thenReturn(Mono.just(true));
        // Nobody is deleted unless a test says so.
        lenient().when(markers.isDeleted(anyString())).thenReturn(Mono.just(false));
        lenient().when(markers.findDeletedAmong(any())).thenReturn(Flux.empty());
    }

    /** Lenient: each path asks the markers one way or the other. */
    private void deleted(String... userIds) {
        lenient().when(markers.isDeleted(anyString()))
                .thenAnswer(call -> Mono.just(List.of(userIds).contains(call.getArgument(0))));
        lenient().when(markers.findDeletedAmong(any())).thenAnswer(call -> Flux.fromIterable(call.<Collection<String>>getArgument(0))
                .filter(List.of(userIds)::contains));
    }

    private static boolean failsWith(Throwable error, BusinessErrorCode code) {
        return error instanceof BusinessException business && business.getErrorCode() == code;
    }

    @Test
    void readOrCreateConversation_shouldReturnTheExisting_whenBothAreActive() {
        List<String> participants = List.of("user1", "user2");
        ConversationDetail detail = new ConversationDetail(UUID.randomUUID().toString(), participants, null, List.of(),
                0, null, null);

        when(chatRepository.findByParticipantIds(participants)).thenReturn(Mono.just(detail));

        StepVerifier.create(chatService.readOrCreateConversation("user1", "user2"))
                .expectNext(detail)
                .expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void readOrCreateConversation_shouldCreate_whenBothAreActiveAndNoneExists() {
        List<String> participants = List.of("user1", "user2");
        ConversationDetail detail = new ConversationDetail(UUID.randomUUID().toString(), participants, null, List.of(),
                0, null, null);

        when(chatRepository.findByParticipantIds(participants)).thenReturn(Mono.empty());
        when(chatRepository.saveWithInitialBucket(participants)).thenReturn(Mono.just(detail));

        StepVerifier.create(chatService.readOrCreateConversation("user1", "user2"))
                .expectNext(detail)
                .expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    @DisplayName("a stranger gets no private conversation: nothing is created")
    void readOrCreateConversation_shouldRefuseWithFriendshipRequired_andCreateNothing_whenTheyAreNotFriends() {
        when(friendships.areFriends("user1", "user2")).thenReturn(Mono.just(false));
        when(chatRepository.findByParticipantIds(List.of("user1", "user2"))).thenReturn(Mono.empty());

        StepVerifier.create(chatService.readOrCreateConversation("user1", "user2"))
                .expectErrorMatches(error -> failsWith(error, BusinessErrorCode.FRIENDSHIP_REQUIRED))
                .verify(Duration.ofSeconds(10));

        verify(chatRepository, never()).saveWithInitialBucket(any());
    }

    @Test
    void readOrCreateConversation_shouldRefuseWithFriendshipRequired_whenTheFriendIsOneself() {
        when(chatRepository.findByParticipantIds(List.of("user1", "user1"))).thenReturn(Mono.empty());

        StepVerifier.create(chatService.readOrCreateConversation("user1", "user1"))
                .expectErrorMatches(error -> failsWith(error, BusinessErrorCode.FRIENDSHIP_REQUIRED))
                .verify(Duration.ofSeconds(10));

        verify(chatRepository, never()).saveWithInitialBucket(any());
    }

    @Test
    @DisplayName("the history with a former friend stays readable")
    void readOrCreateConversation_shouldReturnTheExisting_whenTheyAreNoLongerFriends() {
        lenient().when(friendships.areFriends("user1", "user2")).thenReturn(Mono.just(false));
        ConversationDetail detail = new ConversationDetail("conv-1", List.of("user1", "user2"), ConversationType.DIRECT,
                List.of(), 0, null, null);
        when(chatRepository.findByParticipantIds(List.of("user1", "user2"))).thenReturn(Mono.just(detail));

        StepVerifier.create(chatService.readOrCreateConversation("user1", "user2")).expectNext(detail).expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    @DisplayName("the survivor still opens the conversation with a deleted friend, read-only")
    void readOrCreateConversation_shouldReturnTheExistingConversation_whenTheFriendIsDeleted() {
        deleted("user2");
        ConversationDetail detail = new ConversationDetail("conv-1", List.of("user1", "user2"), ConversationType.DIRECT,
                List.of(), 0, null, null, null, true);
        when(chatRepository.findByParticipantIds(List.of("user1", "user2"))).thenReturn(Mono.just(detail));

        StepVerifier.create(chatService.readOrCreateConversation("user1", "user2")).expectNext(detail).expectComplete().verify(Duration.ofSeconds(10));

        verify(chatRepository, never()).saveWithInitialBucket(any());
    }

    @Test
    void readOrCreateConversation_shouldAnswerNotFound_andCreateNothing_whenTheFriendIsDeletedAndNoConversationExists() {
        deleted("user2");
        when(chatRepository.findByParticipantIds(List.of("user1", "user2"))).thenReturn(Mono.empty());

        StepVerifier.create(chatService.readOrCreateConversation("user1", "user2"))
                .expectErrorMatches(error -> failsWith(error, BusinessErrorCode.CONVERSATION_NOT_FOUND))
                .verify(Duration.ofSeconds(10));

        verify(chatRepository, never()).saveWithInitialBucket(any());
    }

    @Test
    void readOrCreateConversation_shouldRefuseWithAccountDeleted_whenTheRequesterIsDeleted() {
        deleted("user1");

        StepVerifier.create(chatService.readOrCreateConversation("user1", "user2"))
                .expectErrorMatches(error -> failsWith(error, BusinessErrorCode.ACCOUNT_DELETED))
                .verify(Duration.ofSeconds(10));

        verify(chatRepository, never()).findByParticipantIds(any());
        verify(chatRepository, never()).saveWithInitialBucket(any());
    }

    private static ConversationDetail direct(boolean readOnly) {
        return new ConversationDetail("conv-1", List.of("caller", "friend"), ConversationType.DIRECT, null, null,
                null, null, null, readOnly);
    }

    private void memberOf(String senderId) {
        when(chatRepository.findMembership("conv-1", senderId)).thenReturn(Mono.just(direct(false)));
    }

    private static Message messageFrom(String senderId) {
        return new Message("m-1", senderId, "Caller", "whoever-the-client-said",
                "conv-1", "hello", Instant.parse("2026-10-12T19:00:00Z"));
    }

    private static LiveMessage addressedTo(Message message, String recipientId) {
        return addressedTo(message, recipientId, BroadcastKind.NEW);
    }

    private static LiveMessage addressedTo(Message message, String recipientId, BroadcastKind kind) {
        return new LiveMessage(new Message(message.id(), message.senderId(), message.senderName(), recipientId,
                message.conversationId(), message.content(), message.timestamp(), message.photo()), kind);
    }

    @Test
    @DisplayName("a message reaches every other member, whoever the client named as receiver")
    void sendMessage_shouldBroadcastToEveryOtherMember() {
        Message message = messageFrom("caller");
        memberOf("caller");
        when(chatRepository.postMessage(message)).thenReturn(Mono.just(List.of("caller", "ines", "hugo")));
        when(messageBroadcaster.broadcast(any())).thenReturn(Mono.empty());

        StepVerifier.create(chatService.sendMessage(message)).expectNext(message).expectComplete().verify(Duration.ofSeconds(10));

        verify(messageBroadcaster).broadcast(addressedTo(message, "ines"));
        verify(messageBroadcaster).broadcast(addressedTo(message, "hugo"));
        verify(messageBroadcaster, never()).broadcast(addressedTo(message, "caller"));
        verify(messageBroadcaster, never()).broadcast(new LiveMessage(message, BroadcastKind.NEW));
    }

    @Test
    @DisplayName("a message sent without an id gets one, stored and broadcast with it")
    void sendMessage_shouldAssignAnId_whenTheClientSentNone() {
        Message withoutId = new Message(null, "caller", "Caller", null, "conv-1", "hello",
                Instant.parse("2026-10-12T19:00:00Z"));
        memberOf("caller");
        when(chatRepository.postMessage(any())).thenReturn(Mono.just(List.of("caller", "ines")));
        when(messageBroadcaster.broadcast(any())).thenReturn(Mono.empty());

        StepVerifier.create(chatService.sendMessage(withoutId))
                .assertNext(sent -> {
                    assertThat(sent.id()).isNotBlank();
                    assertThat(sent.content()).isEqualTo("hello");
                })
                .expectComplete().verify(Duration.ofSeconds(10));
        verify(chatRepository).postMessage(argThat(stored -> stored.id() != null));
        verify(messageBroadcaster).broadcast(argThat(copy -> copy.message().id() != null
                && "ines".equals(copy.message().receiverId()) && copy.kind() == BroadcastKind.NEW));
    }

    @Test
    @DisplayName("a message is persisted before it is broadcast, never the other way round")
    void sendMessage_shouldPersistBeforeBroadcasting() {
        Message message = messageFrom("caller");
        memberOf("caller");
        InOrder order = inOrder(chatRepository, messageBroadcaster);

        when(chatRepository.postMessage(message)).thenReturn(Mono.just(List.of("caller", "friend")));
        when(messageBroadcaster.broadcast(any())).thenReturn(Mono.empty());

        StepVerifier.create(chatService.sendMessage(message)).expectNext(message).expectComplete().verify(Duration.ofSeconds(10));

        order.verify(chatRepository).postMessage(message);
        order.verify(messageBroadcaster).broadcast(addressedTo(message, "friend"));
    }

    @Test
    @DisplayName("a failed write is not broadcast: no message shown that does not exist")
    void sendMessage_shouldNotBroadcastWhenThePersistFails() {
        Message message = messageFrom("caller");
        memberOf("caller");

        when(chatRepository.postMessage(message))
                .thenReturn(Mono.error(new IllegalStateException("mongo down")));

        StepVerifier.create(chatService.sendMessage(message))
                .expectError(IllegalStateException.class)
                .verify(Duration.ofSeconds(10));

        verify(messageBroadcaster, never()).broadcast(any());
    }

    @Test
    @DisplayName("a sender outside the conversation is refused, so the client shows the message as failed")
    void sendMessage_shouldFailAndReachNoOne_whenTheSenderIsNotAMember() {
        Message message = messageFrom("intruder");
        when(chatRepository.findMembership("conv-1", "intruder")).thenReturn(Mono.empty());

        StepVerifier.create(chatService.sendMessage(message))
                .expectErrorMatches(error -> error instanceof BusinessException
                        && ((BusinessException) error).getErrorCode() == BusinessErrorCode.CONVERSATION_NOT_FOUND)
                .verify(Duration.ofSeconds(10));

        verify(chatRepository, never()).postMessage(any());
        verify(messageBroadcaster, never()).broadcast(any());
    }

    @Test
    @DisplayName("a sender who left between the membership check and the write is refused all the same")
    void sendMessage_shouldFail_whenTheWriteFindsNoMembership() {
        Message message = messageFrom("caller");
        memberOf("caller");
        when(chatRepository.postMessage(message)).thenReturn(Mono.empty());

        StepVerifier.create(chatService.sendMessage(message))
                .expectErrorMatches(error -> failsWith(error, BusinessErrorCode.CONVERSATION_NOT_FOUND))
                .verify(Duration.ofSeconds(10));

        verify(messageBroadcaster, never()).broadcast(any());
    }

    @Test
    @DisplayName("a deleted sender writes nothing, even with a token still valid")
    void sendMessage_shouldRefuseWithAccountDeleted_andWriteNothing_whenTheSenderIsDeleted() {
        deleted("caller");

        StepVerifier.create(chatService.sendMessage(messageFrom("caller")))
                .expectErrorMatches(error -> failsWith(error, BusinessErrorCode.ACCOUNT_DELETED))
                .verify(Duration.ofSeconds(10));

        verify(chatRepository, never()).findMembership(any(), any());
        verify(chatRepository, never()).postMessage(any());
        verify(messageBroadcaster, never()).broadcast(any());
    }

    @Test
    @DisplayName("nobody writes in a private conversation whose other participant deleted their account")
    void sendMessage_shouldRefuseWithReadOnly_andWriteNothing_whenTheConversationIsReadOnly() {
        when(chatRepository.findMembership("conv-1", "caller")).thenReturn(Mono.just(direct(true)));

        StepVerifier.create(chatService.sendMessage(messageFrom("caller")))
                .expectErrorMatches(error -> failsWith(error, BusinessErrorCode.CONVERSATION_READ_ONLY))
                .verify(Duration.ofSeconds(10));

        verify(chatRepository, never()).postMessage(any());
        verify(messageBroadcaster, never()).broadcast(any());
    }

    @Test
    @DisplayName("nobody writes in a private conversation with someone who is not their friend")
    void sendMessage_shouldRefuseWithFriendshipRequired_andWriteNothing_whenTheyAreNotFriends() {
        memberOf("caller");
        when(friendships.areFriends("caller", "friend")).thenReturn(Mono.just(false));

        StepVerifier.create(chatService.sendMessage(messageFrom("caller")))
                .expectErrorMatches(error -> failsWith(error, BusinessErrorCode.FRIENDSHIP_REQUIRED))
                .verify(Duration.ofSeconds(10));

        verify(chatRepository, never()).postMessage(any());
        verify(messageBroadcaster, never()).broadcast(any());
    }

    @Test
    void requireFriendsInDirect_shouldComplete_whenTheyAreFriends() {
        StepVerifier.create(chatService.requireFriendsInDirect(direct(false), "caller")).expectComplete().verify(Duration.ofSeconds(10));

        verify(friendships).areFriends("caller", "friend");
    }

    @Test
    void requireFriendsInDirect_shouldFailWithFriendshipRequired_whenTheyAreNotFriends() {
        when(friendships.areFriends("caller", "friend")).thenReturn(Mono.just(false));

        StepVerifier.create(chatService.requireFriendsInDirect(direct(false), "caller"))
                .expectErrorMatches(error -> failsWith(error, BusinessErrorCode.FRIENDSHIP_REQUIRED))
                .verify(Duration.ofSeconds(10));
    }

    @Test
    @DisplayName("an event's group needs no friendship between its members")
    void requireFriendsInDirect_shouldCompleteWithoutAsking_forAnEventGroup() {
        ConversationDetail group = new ConversationDetail("conv-2", List.of("caller", "stranger"), ConversationType.EVENT,
                List.of(), 0, "Quiz", "event-1");

        StepVerifier.create(chatService.requireFriendsInDirect(group, "caller")).expectComplete().verify(Duration.ofSeconds(10));

        verify(friendships, never()).areFriends(anyString(), anyString());
    }

    @Test
    @DisplayName("a past event's group keeps a deleted member's id, but no message is addressed to it")
    void sendMessage_shouldNotBroadcastToDeletedMembers() {
        Message message = messageFrom("caller");
        memberOf("caller");
        deleted("gone");
        when(chatRepository.postMessage(message)).thenReturn(Mono.just(List.of("caller", "gone", "ines")));
        when(messageBroadcaster.broadcast(any())).thenReturn(Mono.empty());

        StepVerifier.create(chatService.sendMessage(message)).expectNext(message).expectComplete().verify(Duration.ofSeconds(10));

        verify(messageBroadcaster).broadcast(addressedTo(message, "ines"));
        verify(messageBroadcaster, times(1)).broadcast(any());
    }

    @Test
    @DisplayName("a stored message is acknowledged even when the markers cannot be read: a retry would store it twice")
    void sendMessage_shouldStillAcknowledge_whenTheMarkersCannotBeRead() {
        Message message = messageFrom("caller");
        memberOf("caller");
        when(chatRepository.postMessage(message)).thenReturn(Mono.just(List.of("caller", "ines")));
        when(markers.findDeletedAmong(any())).thenReturn(Flux.error(new TechnicalException(TechnicalErrorCode.STORE_UNAVAILABLE)));

        StepVerifier.create(chatService.sendMessage(message)).expectNext(message).expectComplete().verify(Duration.ofSeconds(10));

        verify(chatRepository).postMessage(message);
        verify(messageBroadcaster, never()).broadcast(any());
    }

    @Test
    @DisplayName("the stream is the broadcaster's, so it spans instances")
    void streamMessages_shouldDelegateToTheBroadcaster() {
        String userId = "user1";
        LiveMessage message = new LiveMessage(new Message(UUID.randomUUID().toString(), "caller", "Caller", userId,
                UUID.randomUUID().toString(), "hello", Instant.now()), BroadcastKind.NEW);

        // The service no longer owns a sink. That sink was local to one JVM, so a message
        // accepted by one pod never reached a recipient connected to another.
        when(messageBroadcaster.subscribe(userId)).thenReturn(Flux.just(message));

        StepVerifier.create(chatService.streamMessages(userId))
                .expectNext(message)
                .expectComplete().verify(Duration.ofSeconds(10));

        verify(messageBroadcaster).subscribe(userId);
    }

    private static Message photoRemovedBy(String senderId) {
        return new Message("m-1", senderId, "Alice", null, "conv-1", "Podium",
                Instant.parse("2026-10-12T19:00:00Z"), Photo.REMOVED);
    }

    private void members(String requesterId, String... memberIds) {
        when(chatRepository.findById("conv-1", requesterId)).thenReturn(Mono.just(new ConversationDetail("conv-1",
                List.of(memberIds), ConversationType.EVENT, List.of(), 0, "Board games", "event-1")));
    }

    @Test
    @DisplayName("an update reaches every member but the one who made it, marked UPDATED")
    void broadcastUpdate_shouldReachEveryMemberButTheRequester() {
        Message updated = photoRemovedBy("alice");
        members("olivia", "olivia", "alice", "bob");
        when(messageBroadcaster.broadcast(any())).thenReturn(Mono.empty());

        StepVerifier.create(chatService.broadcastUpdate(updated, "olivia")).expectComplete().verify(Duration.ofSeconds(10));

        verify(messageBroadcaster).broadcast(addressedTo(updated, "alice", BroadcastKind.UPDATED));
        verify(messageBroadcaster).broadcast(addressedTo(updated, "bob", BroadcastKind.UPDATED));
        verify(messageBroadcaster, never()).broadcast(addressedTo(updated, "olivia", BroadcastKind.UPDATED));
        verify(messageBroadcaster, times(2)).broadcast(any());
    }

    @Test
    @DisplayName("a recipient whose broadcast fails is skipped; the others still get the update")
    void broadcastUpdate_shouldSurviveABroadcasterError() {
        Message updated = photoRemovedBy("alice");
        members("alice", "alice", "bob", "carol");
        when(messageBroadcaster.broadcast(addressedTo(updated, "bob", BroadcastKind.UPDATED)))
                .thenReturn(Mono.error(new IllegalStateException("broker down")));
        when(messageBroadcaster.broadcast(addressedTo(updated, "carol", BroadcastKind.UPDATED)))
                .thenReturn(Mono.empty());

        StepVerifier.create(chatService.broadcastUpdate(updated, "alice")).expectComplete().verify(Duration.ofSeconds(10));

        verify(messageBroadcaster).broadcast(addressedTo(updated, "carol", BroadcastKind.UPDATED));
    }

    @Test
    void broadcastUpdate_shouldSkipDeletedMembers() {
        Message updated = photoRemovedBy("alice");
        members("olivia", "olivia", "alice", "gone");
        deleted("gone");
        when(messageBroadcaster.broadcast(any())).thenReturn(Mono.empty());

        StepVerifier.create(chatService.broadcastUpdate(updated, "olivia")).expectComplete().verify(Duration.ofSeconds(10));

        verify(messageBroadcaster).broadcast(addressedTo(updated, "alice", BroadcastKind.UPDATED));
        verify(messageBroadcaster, times(1)).broadcast(any());
    }

    @Test
    void broadcastUpdate_shouldStillComplete_whenTheMarkersCannotBeRead() {
        members("olivia", "olivia", "alice");
        when(markers.findDeletedAmong(any())).thenReturn(Flux.error(new TechnicalException(TechnicalErrorCode.STORE_UNAVAILABLE)));

        StepVerifier.create(chatService.broadcastUpdate(photoRemovedBy("alice"), "olivia")).expectComplete().verify(Duration.ofSeconds(10));

        verify(messageBroadcaster, never()).broadcast(any());
    }

    @Test
    void broadcastUpdate_shouldSendNothing_whenTheRequesterIsNotAMember() {
        when(chatRepository.findById("conv-1", "mallory")).thenReturn(Mono.empty());

        StepVerifier.create(chatService.broadcastUpdate(photoRemovedBy("alice"), "mallory")).expectComplete().verify(Duration.ofSeconds(10));

        verify(messageBroadcaster, never()).broadcast(any());
    }

    @Test
    void readPreviousMessages_Success() {
        String convId = UUID.randomUUID().toString();
        MessageBucket bucket = new MessageBucket(convId, 0, List.of());

        when(chatRepository.findBucketByConversationIdAndBucketIndex(convId, 0, "user1"))
                .thenReturn(Mono.just(bucket));

        StepVerifier.create(chatService.readPreviousMessages(convId, 0, "user1"))
                .expectNext(bucket)
                .expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void readPreviousMessages_shouldBeEmptyForNonParticipant() {
        String convId = UUID.randomUUID().toString();

        when(chatRepository.findBucketByConversationIdAndBucketIndex(convId, 0, "intruder"))
                .thenReturn(Mono.empty());

        StepVerifier.create(chatService.readPreviousMessages(convId, 0, "intruder"))
                .expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void readConversationById_Success() {
        String convId = UUID.randomUUID().toString();
        ConversationDetail detail = new ConversationDetail(convId, List.of(), null, List.of(), 0, null, null);

        when(chatRepository.findById(convId, "user1")).thenReturn(Mono.just(detail));

        StepVerifier.create(chatService.readConversationById(convId, "user1"))
                .expectNext(detail)
                .expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void readConversationById_shouldBeEmptyForNonParticipant() {
        String convId = UUID.randomUUID().toString();

        when(chatRepository.findById(convId, "intruder")).thenReturn(Mono.empty());

        StepVerifier.create(chatService.readConversationById(convId, "intruder"))
                .expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void readConversations_Success() {
        String userId = "user1";
        ConversationSummary summary = new ConversationSummary(UUID.randomUUID().toString(), List.of("user1", "user2"), null, "Title",
                Instant.now(), null, null, false);

        when(chatRepository.findUserConversations(userId)).thenReturn(Flux.just(summary));

        StepVerifier.create(chatService.readConversations(userId))
                .expectNext(summary)
                .expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void readEventConversation_shouldDelegateScopedToTheRequester() {
        ConversationDetail detail = new ConversationDetail("conv-1", List.of("me"), null, List.of(), 0, null, null);
        when(chatRepository.findByEventId("event-1", "me")).thenReturn(Mono.just(detail));

        StepVerifier.create(chatService.readEventConversation("event-1", "me")).expectNext(detail).expectComplete().verify(Duration.ofSeconds(10));
    }

    // --- system notices ---------------------------------------------------

    private static final String NOTICE = "📅 Date fixée : jeudi 16 octobre, 19:00 – 21:00";

    /** What the repository answers: the notice as stored in the event's group. */
    private void groupStores(String eventId, boolean duplicate, String... memberIds) {
        when(chatRepository.postSystemMessage(org.mockito.ArgumentMatchers.eq(eventId), any()))
                .thenAnswer(call -> {
                    Message notice = call.getArgument(1);
                    Message stored = new Message(notice.id(), notice.senderId(), notice.senderName(), null,
                            "conv-9", notice.content(), notice.timestamp(), null, notice.type());
                    return Mono.just(new PostedMessage(stored, List.of(memberIds), duplicate));
                });
    }

    @Test
    @DisplayName("a notice is a SYSTEM message from the application, stamped by the clock")
    void postSystemNotice_shouldPostASystemMessageSignedByTheApplication() {
        groupStores("event-1", false, "ines");
        when(messageBroadcaster.broadcast(any())).thenReturn(Mono.empty());

        StepVerifier.create(chatService.postSystemNotice("event-1", NOTICE))
                .assertNext(posted -> {
                    assertThat(posted.type()).isEqualTo(MessageType.SYSTEM);
                    assertThat(posted.senderId()).isEqualTo(Message.SYSTEM_SENDER_ID);
                    assertThat(posted.senderName()).isEqualTo("Wely");
                    assertThat(posted.content()).isEqualTo(NOTICE);
                    assertThat(posted.timestamp()).isEqualTo(NOW);
                    assertThat(posted.conversationId()).isEqualTo("conv-9");
                    assertThat(posted.id()).isNotBlank();
                })
                .expectComplete().verify(Duration.ofSeconds(10));
        // Through the system path, never postMessage and its sender check.
        verify(chatRepository, never()).postMessage(any());
    }

    @Test
    @DisplayName("a notice goes into any conversation by its id, and reaches its members")
    void postConversationNotice_shouldPostIntoTheConversation_andPushToItsMembers() {
        when(chatRepository.postSystemMessageToConversation(org.mockito.ArgumentMatchers.eq("conv-3"), any()))
                .thenAnswer(call -> {
                    Message notice = call.getArgument(1);
                    return Mono.just(new PostedMessage(notice, List.of("alice", "bob"), false));
                });
        when(messageBroadcaster.broadcast(any())).thenReturn(Mono.empty());

        StepVerifier.create(chatService.postConversationNotice("conv-3", "call:c1", "📞 Appel audio manqué"))
                .assertNext(posted -> {
                    assertThat(posted.type()).isEqualTo(MessageType.SYSTEM);
                    assertThat(posted.content()).isEqualTo("📞 Appel audio manqué");
                })
                .expectComplete().verify(Duration.ofSeconds(10));

        verify(messageBroadcaster, times(2)).broadcast(any());
    }

    @Test
    @DisplayName("the same key posts once: a notice already there is not pushed again")
    void postConversationNotice_shouldPushNothing_forADuplicate() {
        when(chatRepository.postSystemMessageToConversation(org.mockito.ArgumentMatchers.eq("conv-3"), any()))
                .thenAnswer(call -> Mono.just(new PostedMessage(call.getArgument(1), List.of("alice"), true)));

        StepVerifier.create(chatService.postConversationNotice("conv-3", "call:c1", "x")).expectNextCount(1)
                .expectComplete().verify(Duration.ofSeconds(10));

        verify(messageBroadcaster, never()).broadcast(any());
    }

    @Test
    void postConversationNotice_shouldFail_whenTheConversationDoesNotExist() {
        when(chatRepository.postSystemMessageToConversation(org.mockito.ArgumentMatchers.eq("gone"), any()))
                .thenReturn(Mono.empty());

        StepVerifier.create(chatService.postConversationNotice("gone", "call:c1", "x"))
                .expectErrorMatches(error -> error instanceof BusinessException business
                        && business.getErrorCode() == BusinessErrorCode.CONVERSATION_NOT_FOUND)
                .verify(Duration.ofSeconds(10));
    }

    @Test
    @DisplayName("a notice reaches every member, the organizer included, marked NEW")
    void postSystemNotice_shouldBroadcastToEveryParticipant() {
        groupStores("event-1", false, "olivia", "ines", "hugo");
        when(messageBroadcaster.broadcast(any())).thenReturn(Mono.empty());

        StepVerifier.create(chatService.postSystemNotice("event-1", NOTICE)).expectNextCount(1).expectComplete().verify(Duration.ofSeconds(10));

        for (String member : List.of("olivia", "ines", "hugo")) {
            verify(messageBroadcaster).broadcast(argThat(live -> member.equals(live.message().receiverId())
                    && live.kind() == BroadcastKind.NEW && live.message().type() == MessageType.SYSTEM
                    && "conv-9".equals(live.message().conversationId())));
        }
        verify(messageBroadcaster, times(3)).broadcast(any());
    }

    @Test
    void postSystemNotice_shouldSkipDeletedMembers() {
        groupStores("event-1", false, "olivia", "gone");
        deleted("gone");
        when(messageBroadcaster.broadcast(any())).thenReturn(Mono.empty());

        StepVerifier.create(chatService.postSystemNotice("event-1", NOTICE)).expectNextCount(1).expectComplete().verify(Duration.ofSeconds(10));

        verify(messageBroadcaster).broadcast(argThat(live -> "olivia".equals(live.message().receiverId())));
        verify(messageBroadcaster, times(1)).broadcast(any());
    }

    @Test
    @DisplayName("the notice is stored: markers that cannot be read cost its live push, not the notice")
    void postSystemNotice_shouldStillPostTheNotice_whenTheMarkersCannotBeRead() {
        groupStores("event-1", false, "olivia", "bob");
        when(markers.findDeletedAmong(any())).thenReturn(Flux.error(new TechnicalException(TechnicalErrorCode.STORE_UNAVAILABLE)));

        StepVerifier.create(chatService.postSystemNotice("event-1", NOTICE))
                .assertNext(notice -> assertThat(notice.content()).isEqualTo(NOTICE))
                .expectComplete().verify(Duration.ofSeconds(10));

        verify(messageBroadcaster, never()).broadcast(any());
    }

    @Test
    void postConversationNotice_shouldSkipDeletedMembers() {
        when(chatRepository.postSystemMessageToConversation(org.mockito.ArgumentMatchers.eq("conv-3"), any()))
                .thenAnswer(call -> Mono.just(new PostedMessage(call.getArgument(1), List.of("alice", "gone"), false)));
        deleted("gone");
        when(messageBroadcaster.broadcast(any())).thenReturn(Mono.empty());

        StepVerifier.create(chatService.postConversationNotice("conv-3", "call:c1", "📞 Appel audio · 3 min"))
                .expectNextCount(1)
                .expectComplete().verify(Duration.ofSeconds(10));

        verify(messageBroadcaster).broadcast(argThat(live -> "alice".equals(live.message().receiverId())));
        verify(messageBroadcaster, times(1)).broadcast(any());
    }

    @Test
    @DisplayName("the same event and text give the same id, so a redelivered record is recognised")
    void postSystemNotice_shouldDeriveTheIdFromTheEventAndTheText() {
        groupStores("event-1", false);
        groupStores("event-2", false);

        StepVerifier.create(Mono.zip(
                        chatService.postSystemNotice("event-1", NOTICE).map(Message::id),
                        chatService.postSystemNotice("event-1", NOTICE).map(Message::id),
                        chatService.postSystemNotice("event-1", NOTICE + "!").map(Message::id),
                        chatService.postSystemNotice("event-2", NOTICE).map(Message::id)))
                .assertNext(ids -> {
                    assertThat(ids.getT2()).isEqualTo(ids.getT1());
                    assertThat(ids.getT3()).isNotEqualTo(ids.getT1());
                    assertThat(ids.getT4()).isNotEqualTo(ids.getT1());
                })
                .expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    @DisplayName("a notice already stored is not pushed again")
    void postSystemNotice_shouldNotBroadcast_whenTheNoticeWasAlreadyStored() {
        groupStores("event-1", true, "olivia", "ines");

        StepVerifier.create(chatService.postSystemNotice("event-1", NOTICE)).expectNextCount(1).expectComplete().verify(Duration.ofSeconds(10));

        verify(messageBroadcaster, never()).broadcast(any());
    }

    @Test
    @DisplayName("no group yet is an error, so the consumer retries once it exists")
    void postSystemNotice_shouldFail_whenTheEventHasNoGroupYet() {
        when(chatRepository.postSystemMessage(org.mockito.ArgumentMatchers.eq("event-1"), any()))
                .thenReturn(Mono.empty());

        StepVerifier.create(chatService.postSystemNotice("event-1", NOTICE))
                .expectErrorMatches(error -> error instanceof BusinessException
                        && ((BusinessException) error).getErrorCode() == BusinessErrorCode.CONVERSATION_NOT_FOUND)
                .verify(Duration.ofSeconds(10));

        verify(messageBroadcaster, never()).broadcast(any());
    }

    @Test
    void postSystemNotice_shouldPropagateAStorageError() {
        when(chatRepository.postSystemMessage(org.mockito.ArgumentMatchers.eq("event-1"), any()))
                .thenReturn(Mono.error(new IllegalStateException("mongo down")));

        StepVerifier.create(chatService.postSystemNotice("event-1", NOTICE))
                .expectError(IllegalStateException.class)
                .verify(Duration.ofSeconds(10));

        verify(messageBroadcaster, never()).broadcast(any());
    }

    @Test
    @DisplayName("a member whose broadcast fails is skipped; the notice is stored and the others get it")
    void postSystemNotice_shouldSurviveABroadcasterError() {
        groupStores("event-1", false, "ines", "hugo");
        when(messageBroadcaster.broadcast(argThat(live -> live != null && "ines".equals(live.message().receiverId()))))
                .thenReturn(Mono.error(new IllegalStateException("broker down")));
        when(messageBroadcaster.broadcast(argThat(live -> live != null && "hugo".equals(live.message().receiverId()))))
                .thenReturn(Mono.empty());

        StepVerifier.create(chatService.postSystemNotice("event-1", NOTICE)).expectNextCount(1).expectComplete().verify(Duration.ofSeconds(10));

        verify(messageBroadcaster, times(2)).broadcast(any());
    }
}
