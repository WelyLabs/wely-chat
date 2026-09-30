package com.calendar.chat.domain.services;

import com.calendar.chat.domain.models.ConversationDetail;
import com.calendar.chat.domain.models.ConversationSummary;
import com.calendar.chat.domain.models.Message;
import com.calendar.chat.domain.models.MessageBucket;
import com.calendar.chat.domain.ports.ChatRepository;
import com.calendar.chat.domain.ports.MessageBroadcaster;
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

import java.time.LocalDateTime;
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
                0);

        when(chatRepository.findByParticipantIds(participants)).thenReturn(Mono.just(detail));

        StepVerifier.create(chatService.readOrCreateConversation(participants))
                .expectNext(detail)
                .verifyComplete();
    }

    @Test
    void readOrCreateConversation_New() {
        List<String> participants = List.of("user1", "user2");
        ConversationDetail detail = new ConversationDetail(UUID.randomUUID().toString(), participants, null, List.of(),
                0);

        when(chatRepository.findByParticipantIds(participants)).thenReturn(Mono.empty());
        when(chatRepository.saveWithInitialBucket(participants)).thenReturn(Mono.just(detail));

        StepVerifier.create(chatService.readOrCreateConversation(participants))
                .expectNext(detail)
                .verifyComplete();
    }

    @Test
    void sendMessage_Success() {
        Message message = new Message(UUID.randomUUID().toString(), "caller", "Caller", "receiver",
                UUID.randomUUID().toString(), "hello", LocalDateTime.now());

        when(chatRepository.postMessage(message)).thenReturn(Mono.empty());
        when(messageBroadcaster.broadcast(message)).thenReturn(Mono.empty());

        StepVerifier.create(chatService.sendMessage(message))
                .verifyComplete();
    }

    @Test
    @DisplayName("a message is persisted before it is broadcast, never the other way round")
    void sendMessage_shouldPersistBeforeBroadcasting() {
        Message message = new Message(null, "caller", "Caller", "receiver",
                UUID.randomUUID().toString(), "hello", LocalDateTime.now());
        InOrder order = inOrder(chatRepository, messageBroadcaster);

        when(chatRepository.postMessage(message)).thenReturn(Mono.empty());
        when(messageBroadcaster.broadcast(message)).thenReturn(Mono.empty());

        StepVerifier.create(chatService.sendMessage(message)).verifyComplete();

        order.verify(chatRepository).postMessage(message);
        order.verify(messageBroadcaster).broadcast(message);
    }

    @Test
    @DisplayName("a failed write is not broadcast: no message shown that does not exist")
    void sendMessage_shouldNotBroadcastWhenThePersistFails() {
        Message message = new Message(null, "caller", "Caller", "receiver",
                UUID.randomUUID().toString(), "hello", LocalDateTime.now());

        when(chatRepository.postMessage(message))
                .thenReturn(Mono.error(new IllegalStateException("mongo down")));

        StepVerifier.create(chatService.sendMessage(message))
                .expectError(IllegalStateException.class)
                .verify();

        verify(messageBroadcaster, never()).broadcast(any());
    }

    @Test
    @DisplayName("the stream is the broadcaster's, so it spans instances")
    void streamMessages_shouldDelegateToTheBroadcaster() {
        String userId = "user1";
        Message message = new Message(UUID.randomUUID().toString(), "caller", "Caller", userId,
                UUID.randomUUID().toString(), "hello", LocalDateTime.now());

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
        ConversationDetail detail = new ConversationDetail(convId, List.of(), null, List.of(), 0);

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
        ConversationSummary summary = new ConversationSummary(UUID.randomUUID().toString(), null, "Title",
                LocalDateTime.now(), null);

        when(chatRepository.findUserConversations(userId)).thenReturn(Flux.just(summary));

        StepVerifier.create(chatService.readConversations(userId))
                .expectNext(summary)
                .verifyComplete();
    }
}
