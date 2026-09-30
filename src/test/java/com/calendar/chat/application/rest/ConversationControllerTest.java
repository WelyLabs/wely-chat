package com.calendar.chat.application.rest;

import com.calendar.chat.domain.models.ConversationDetail;
import com.calendar.chat.domain.models.ConversationSummary;
import com.calendar.chat.domain.models.MessageBucket;
import com.calendar.chat.domain.services.ChatService;
import com.calendar.chat.exception.ChatErrorCode;
import com.calendar.chat.exception.ChatException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.security.oauth2.jwt.Jwt;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ConversationControllerTest {

    private static final String CALLER_ID = "caller-1";
    private static final String CONVERSATION_ID = "conv-1";

    @Mock private ChatService chatService;
    @Mock private Jwt jwt;

    private ConversationController controller;

    @BeforeEach
    void setUp() {
        controller = new ConversationController(chatService);
    }

    private void givenAuthenticatedCaller() {
        when(jwt.getClaimAsString("businessId")).thenReturn(CALLER_ID);
    }

    // --- getConversation ---------------------------------------------------

    @Test
    @DisplayName("identity comes from the token, never from a client parameter")
    void getConversation_shouldDeriveTheCallerFromTheToken() {
        givenAuthenticatedCaller();
        ConversationDetail detail = new ConversationDetail();
        when(chatService.readOrCreateConversation(List.of(CALLER_ID, "friend-1")))
                .thenReturn(Mono.just(detail));

        StepVerifier.create(controller.getConversation(jwt, "friend-1"))
                .assertNext(response -> assertThat(response.getBody()).isSameAs(detail))
                .verifyComplete();

        verify(chatService).readOrCreateConversation(List.of(CALLER_ID, "friend-1"));
    }

    // --- readPreviousMessages ---------------------------------------------

    @Test
    void readPreviousMessages_shouldReturnTheRequestedBucket() {
        givenAuthenticatedCaller();
        MessageBucket bucket = new MessageBucket(CONVERSATION_ID, 2, List.of());
        when(chatService.readPreviousMessages(CONVERSATION_ID, 2, CALLER_ID))
                .thenReturn(Mono.just(bucket));

        StepVerifier.create(controller.readPreviousMessages(jwt, CONVERSATION_ID, 2))
                .assertNext(response -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
                    assertThat(response.getBody()).isSameAs(bucket);
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("a non-participant gets a 404 error, without confirming the page exists")
    void readPreviousMessages_shouldFailAsNotFoundForNonParticipant() {
        givenAuthenticatedCaller();
        when(chatService.readPreviousMessages(CONVERSATION_ID, 2, CALLER_ID)).thenReturn(Mono.empty());

        StepVerifier.create(controller.readPreviousMessages(jwt, CONVERSATION_ID, 2))
                .expectErrorMatches(error -> error instanceof ChatException
                        && ((ChatException) error).getErrorCode()
                                == ChatErrorCode.MESSAGE_PAGE_NOT_FOUND)
                .verify();
    }

    @Test
    @DisplayName("the token identity is passed to the service, not dropped")
    void readPreviousMessages_shouldScopeTheLookupToTheCaller() {
        givenAuthenticatedCaller();
        when(chatService.readPreviousMessages(CONVERSATION_ID, 0, CALLER_ID)).thenReturn(Mono.empty());

        controller.readPreviousMessages(jwt, CONVERSATION_ID, 0).onErrorComplete().block();

        // Before the fix only the path's conversationId was passed, so any authenticated
        // user could read someone else's history.
        verify(chatService).readPreviousMessages(CONVERSATION_ID, 0, CALLER_ID);
    }

    // --- getConversationById ----------------------------------------------

    @Test
    void getConversationById_shouldReturnTheConversation() {
        givenAuthenticatedCaller();
        ConversationDetail detail = new ConversationDetail();
        when(chatService.readConversationById(CONVERSATION_ID, CALLER_ID)).thenReturn(Mono.just(detail));

        StepVerifier.create(controller.getConversationById(jwt, CONVERSATION_ID))
                .assertNext(response -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
                    assertThat(response.getBody()).isSameAs(detail);
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("a non-participant and a missing conversation yield the same error")
    void getConversationById_shouldFailAsNotFoundForNonParticipant() {
        givenAuthenticatedCaller();
        when(chatService.readConversationById(CONVERSATION_ID, CALLER_ID)).thenReturn(Mono.empty());

        // A distinct code for "not yours" would let anyone enumerate existing
        // conversations: both cases deliberately share CHT-BUS-001.
        StepVerifier.create(controller.getConversationById(jwt, CONVERSATION_ID))
                .expectErrorMatches(error -> error instanceof ChatException
                        && ((ChatException) error).getErrorCode()
                                == ChatErrorCode.CONVERSATION_NOT_FOUND)
                .verify();
    }

    // --- getConversations -------------------------------------------------

    @Test
    void getConversations_shouldListOnlyTheCallersConversations() {
        givenAuthenticatedCaller();
        ConversationSummary summary =
                new ConversationSummary(CONVERSATION_ID, null, "Title", LocalDateTime.now(), null);
        when(chatService.readConversations(CALLER_ID)).thenReturn(Flux.just(summary));

        var response = controller.getConversations(jwt);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        StepVerifier.create(response.getBody())
                .expectNext(summary)
                .verifyComplete();
        verify(chatService).readConversations(CALLER_ID);
    }
}
