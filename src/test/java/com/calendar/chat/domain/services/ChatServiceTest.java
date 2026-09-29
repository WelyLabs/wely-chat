package com.calendar.chat.domain.services;

import com.calendar.chat.domain.models.ConversationDetail;
import com.calendar.chat.domain.models.ConversationSummary;
import com.calendar.chat.domain.models.Message;
import com.calendar.chat.domain.models.MessageBucket;
import com.calendar.chat.domain.ports.ChatRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ChatServiceTest {

    @Mock
    private ChatRepository chatRepository;

    private ChatService chatService;

    @BeforeEach
    void setUp() {
        chatService = new ChatService(chatRepository);
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

        StepVerifier.create(chatService.sendMessage(message))
                .verifyComplete();
    }

    @Test
    void streamMessages_Success() {
        String userId = "user1";
        Message message = new Message(UUID.randomUUID().toString(), "caller", "Caller", userId,
                UUID.randomUUID().toString(), "hello", LocalDateTime.now());
        Message otherMessage = new Message(UUID.randomUUID().toString(), "caller", "Caller", "other",
                UUID.randomUUID().toString(), "hello", LocalDateTime.now());

        when(chatRepository.postMessage(any(Message.class))).thenReturn(Mono.empty());

        StepVerifier.create(chatService.streamMessages(userId))
                .then(() -> {
                    chatService.sendMessage(message).subscribe();
                    chatService.sendMessage(otherMessage).subscribe();
                })
                .expectNext(message)
                .thenCancel()
                .verify();
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
