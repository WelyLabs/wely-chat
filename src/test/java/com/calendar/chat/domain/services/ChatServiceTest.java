package com.calendar.chat.domain.services;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.argThat;
import com.calendar.chat.domain.models.ConversationDetail;
import com.calendar.chat.domain.models.ConversationSummary;
import com.calendar.chat.domain.models.EventGroup;
import com.calendar.chat.domain.models.Message;
import com.calendar.chat.domain.models.MessageBucket;
import com.calendar.chat.domain.ports.ChatRepository;
import com.calendar.chat.domain.ports.MessageBroadcaster;
import com.calendar.chat.exception.ChatErrorCode;
import com.calendar.chat.exception.ChatException;
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

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ChatServiceTest {

    @Mock
    private ChatRepository chatRepository;

    @Mock
    private MessageBroadcaster messageBroadcaster;

    private ChatService chatService;

    @BeforeEach
    void setUp() {
        chatService = new ChatService(chatRepository, messageBroadcaster);
    }

    @Test
    void readOrCreateConversation_Existing() {
        List<String> participants = List.of("user1", "user2");
        ConversationDetail detail = new ConversationDetail(UUID.randomUUID().toString(), participants, null, List.of(),
                0, null, null);

        when(chatRepository.findByParticipantIds(participants)).thenReturn(Mono.just(detail));

        StepVerifier.create(chatService.readOrCreateConversation(participants))
                .expectNext(detail)
                .verifyComplete();
    }

    @Test
    void readOrCreateConversation_New() {
        List<String> participants = List.of("user1", "user2");
        ConversationDetail detail = new ConversationDetail(UUID.randomUUID().toString(), participants, null, List.of(),
                0, null, null);

        when(chatRepository.findByParticipantIds(participants)).thenReturn(Mono.empty());
        when(chatRepository.saveWithInitialBucket(participants)).thenReturn(Mono.just(detail));

        StepVerifier.create(chatService.readOrCreateConversation(participants))
                .expectNext(detail)
                .verifyComplete();
    }

    private static Message messageFrom(String senderId) {
        return new Message("m-1", senderId, "Caller", "whoever-the-client-said",
                "conv-1", "hello", Instant.parse("2026-10-12T19:00:00Z"));
    }

    private static Message addressedTo(Message message, String recipientId) {
        return new Message(message.id(), message.senderId(), message.senderName(), recipientId,
                message.conversationId(), message.content(), message.timestamp());
    }

    @Test
    @DisplayName("a message reaches every other member, whoever the client named as receiver")
    void sendMessage_shouldBroadcastToEveryOtherMember() {
        Message message = messageFrom("caller");
        when(chatRepository.postMessage(message)).thenReturn(Mono.just(List.of("caller", "ines", "hugo")));
        when(messageBroadcaster.broadcast(any())).thenReturn(Mono.empty());

        StepVerifier.create(chatService.sendMessage(message)).expectNext(message).verifyComplete();

        verify(messageBroadcaster).broadcast(addressedTo(message, "ines"));
        verify(messageBroadcaster).broadcast(addressedTo(message, "hugo"));
        verify(messageBroadcaster, never()).broadcast(addressedTo(message, "caller"));
        verify(messageBroadcaster, never()).broadcast(message);
    }

    @Test
    @DisplayName("a message sent without an id gets one, stored and broadcast with it")
    void sendMessage_shouldAssignAnId_whenTheClientSentNone() {
        Message withoutId = new Message(null, "caller", "Caller", null, "conv-1", "hello",
                Instant.parse("2026-10-12T19:00:00Z"));
        when(chatRepository.postMessage(any())).thenReturn(Mono.just(List.of("caller", "ines")));
        when(messageBroadcaster.broadcast(any())).thenReturn(Mono.empty());

        StepVerifier.create(chatService.sendMessage(withoutId))
                .assertNext(sent -> {
                    assertThat(sent.id()).isNotBlank();
                    assertThat(sent.content()).isEqualTo("hello");
                })
                .verifyComplete();
        verify(chatRepository).postMessage(argThat(stored -> stored.id() != null));
        verify(messageBroadcaster).broadcast(argThat(copy -> copy.id() != null && "ines".equals(copy.receiverId())));
    }

    @Test
    @DisplayName("a message is persisted before it is broadcast, never the other way round")
    void sendMessage_shouldPersistBeforeBroadcasting() {
        Message message = messageFrom("caller");
        InOrder order = inOrder(chatRepository, messageBroadcaster);

        when(chatRepository.postMessage(message)).thenReturn(Mono.just(List.of("caller", "friend")));
        when(messageBroadcaster.broadcast(any())).thenReturn(Mono.empty());

        StepVerifier.create(chatService.sendMessage(message)).expectNext(message).verifyComplete();

        order.verify(chatRepository).postMessage(message);
        order.verify(messageBroadcaster).broadcast(addressedTo(message, "friend"));
    }

    @Test
    @DisplayName("a failed write is not broadcast: no message shown that does not exist")
    void sendMessage_shouldNotBroadcastWhenThePersistFails() {
        Message message = messageFrom("caller");

        when(chatRepository.postMessage(message))
                .thenReturn(Mono.error(new IllegalStateException("mongo down")));

        StepVerifier.create(chatService.sendMessage(message))
                .expectError(IllegalStateException.class)
                .verify();

        verify(messageBroadcaster, never()).broadcast(any());
    }

    @Test
    @DisplayName("a sender outside the conversation is refused, so the client shows the message as failed")
    void sendMessage_shouldFailAndReachNoOne_whenTheSenderIsNotAMember() {
        Message message = messageFrom("intruder");
        when(chatRepository.postMessage(message)).thenReturn(Mono.empty());

        StepVerifier.create(chatService.sendMessage(message))
                .expectErrorMatches(error -> error instanceof ChatException
                        && ((ChatException) error).getErrorCode() == ChatErrorCode.CONVERSATION_NOT_FOUND)
                .verify();

        verify(messageBroadcaster, never()).broadcast(any());
    }

    @Test
    @DisplayName("the stream is the broadcaster's, so it spans instances")
    void streamMessages_shouldDelegateToTheBroadcaster() {
        String userId = "user1";
        Message message = new Message(UUID.randomUUID().toString(), "caller", "Caller", userId,
                UUID.randomUUID().toString(), "hello", Instant.now());

        // The service no longer owns a sink. That sink was local to one JVM, so a message
        // accepted by one pod never reached a recipient connected to another.
        when(messageBroadcaster.subscribe(userId)).thenReturn(Flux.just(message));

        StepVerifier.create(chatService.streamMessages(userId))
                .expectNext(message)
                .verifyComplete();

        verify(messageBroadcaster).subscribe(userId);
    }

    @Test
    void readPreviousMessages_Success() {
        String convId = UUID.randomUUID().toString();
        MessageBucket bucket = new MessageBucket(convId, 0, List.of());

        when(chatRepository.findBucketByConversationIdAndBucketIndex(convId, 0, "user1"))
                .thenReturn(Mono.just(bucket));

        StepVerifier.create(chatService.readPreviousMessages(convId, 0, "user1"))
                .expectNext(bucket)
                .verifyComplete();
    }

    @Test
    void readPreviousMessages_shouldBeEmptyForNonParticipant() {
        String convId = UUID.randomUUID().toString();

        when(chatRepository.findBucketByConversationIdAndBucketIndex(convId, 0, "intruder"))
                .thenReturn(Mono.empty());

        StepVerifier.create(chatService.readPreviousMessages(convId, 0, "intruder"))
                .verifyComplete();
    }

    @Test
    void readConversationById_Success() {
        String convId = UUID.randomUUID().toString();
        ConversationDetail detail = new ConversationDetail(convId, List.of(), null, List.of(), 0, null, null);

        when(chatRepository.findById(convId, "user1")).thenReturn(Mono.just(detail));

        StepVerifier.create(chatService.readConversationById(convId, "user1"))
                .expectNext(detail)
                .verifyComplete();
    }

    @Test
    void readConversationById_shouldBeEmptyForNonParticipant() {
        String convId = UUID.randomUUID().toString();

        when(chatRepository.findById(convId, "intruder")).thenReturn(Mono.empty());

        StepVerifier.create(chatService.readConversationById(convId, "intruder"))
                .verifyComplete();
    }

    @Test
    void readConversations_Success() {
        String userId = "user1";
        ConversationSummary summary = new ConversationSummary(UUID.randomUUID().toString(), List.of("user1", "user2"), null, "Title",
                Instant.now(), null, null);

        when(chatRepository.findUserConversations(userId)).thenReturn(Flux.just(summary));

        StepVerifier.create(chatService.readConversations(userId))
                .expectNext(summary)
                .verifyComplete();
    }

    @Test
    void readEventConversation_shouldDelegateScopedToTheRequester() {
        ConversationDetail detail = new ConversationDetail("conv-1", List.of("me"), null, List.of(), 0, null, null);
        when(chatRepository.findByEventId("event-1", "me")).thenReturn(Mono.just(detail));

        StepVerifier.create(chatService.readEventConversation("event-1", "me")).expectNext(detail).verifyComplete();
    }

    @Test
    void applyEventGroup_shouldSaveTheGroup() {
        EventGroup group = new EventGroup("event-1", "Board games", List.of("ines", "me"), 2);
        when(chatRepository.saveEventGroup(group)).thenReturn(Mono.empty());

        StepVerifier.create(chatService.applyEventGroup(group)).verifyComplete();

        verify(chatRepository).saveEventGroup(group);
    }
}
