package com.calendar.chat.application.rest;

import com.calendar.chat.domain.models.ConversationDetail;
import com.calendar.chat.domain.models.ConversationSummary;
import com.calendar.chat.domain.models.MessageBucket;
import com.calendar.chat.domain.services.ChatService;
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
    @DisplayName("l'identité vient du token, jamais d'un paramètre client")
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
    @DisplayName("un non-participant reçoit 404, sans confirmer que la conversation existe")
    void readPreviousMessages_shouldReturnNotFoundForNonParticipant() {
        givenAuthenticatedCaller();
        when(chatService.readPreviousMessages(CONVERSATION_ID, 2, CALLER_ID)).thenReturn(Mono.empty());

        StepVerifier.create(controller.readPreviousMessages(jwt, CONVERSATION_ID, 2))
                .assertNext(response -> assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND))
                .verifyComplete();
    }

    @Test
    @DisplayName("l'identité du token est transmise au service, pas ignorée")
    void readPreviousMessages_shouldScopeTheLookupToTheCaller() {
        givenAuthenticatedCaller();
        when(chatService.readPreviousMessages(CONVERSATION_ID, 0, CALLER_ID)).thenReturn(Mono.empty());

        controller.readPreviousMessages(jwt, CONVERSATION_ID, 0).block();

        // Avant le correctif, seul le conversationId du chemin était transmis : n'importe
        // quel utilisateur authentifié pouvait lire l'historique d'autrui.
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
    void getConversationById_shouldReturnNotFoundForNonParticipant() {
        givenAuthenticatedCaller();
        when(chatService.readConversationById(CONVERSATION_ID, CALLER_ID)).thenReturn(Mono.empty());

        StepVerifier.create(controller.getConversationById(jwt, CONVERSATION_ID))
                .assertNext(response -> assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND))
                .verifyComplete();
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
