package com.calendar.chat.exception;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

class GlobalErrorHandlerTest {

    private WebTestClient client;

    @RestController
    static class TestController {

        @GetMapping("/not-found")
        Mono<Void> notFound() {
            return Mono.error(new ChatException(ChatErrorCode.CONVERSATION_NOT_FOUND));
        }

        @GetMapping("/forbidden")
        Mono<Void> forbidden() {
            return Mono.error(new ChatException(ChatErrorCode.MESSAGE_PAGE_NOT_FOUND));
        }

        @GetMapping("/generic-error")
        Mono<Void> genericError() {
            return Mono.error(new IllegalStateException("mongodb://admin:hunter2@host/events"));
        }
    }

    @BeforeEach
    void setUp() {
        client = WebTestClient.bindToController(new TestController())
                .controllerAdvice(new GlobalErrorHandler())
                .build();
    }

    @Test
    @DisplayName("a business failure answers as application/problem+json")
    void handleChatException_shouldAnswerAsProblemDetail() {
        client.get().uri("/not-found")
                .exchange()
                .expectStatus().isNotFound()
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON)
                .expectBody()
                .jsonPath("$.status").isEqualTo(404)
                .jsonPath("$.title").isEqualTo("Conversation not found")
                .jsonPath("$.code").isEqualTo("CHT-BUS-001")
                .jsonPath("$.type").isEqualTo("https://welylabs.app/problems/cht-bus-001")
                .jsonPath("$.instance").isEqualTo("/not-found")
                .jsonPath("$.timestamp").exists();
    }

    @Test
    @DisplayName("each code keeps its identity, even at the same status")
    void handleChatException_shouldDistinguishCodesAtTheSameStatus() {
        // Both errors answer 404: only the code tells a client which of the two it got.
        client.get().uri("/forbidden")
                .exchange()
                .expectStatus().isNotFound()
                .expectBody()
                .jsonPath("$.code").isEqualTo("CHT-BUS-002")
                .jsonPath("$.title").isEqualTo("History page not found");
    }

    @Test
    @DisplayName("an unexpected error leaks nothing from the original message")
    void handleUnexpectedException_shouldNotLeakTheCause() {
        client.get().uri("/generic-error")
                .exchange()
                .expectStatus().is5xxServerError()
                .expectBody()
                .jsonPath("$.code").isEqualTo("CHT-TEC-000")
                .jsonPath("$.detail").value(detail -> {
                    // The message carried a Mongo URI with a password.
                    if (detail.toString().contains("hunter2") || detail.toString().contains("mongodb")) {
                        throw new AssertionError("the original message leaked into the response");
                    }
                });
    }
}
