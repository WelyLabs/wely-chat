package com.calendar.chat.exception;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerCodecConfigurer;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.security.access.AccessDeniedException;
import reactor.test.StepVerifier;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class ProblemAccessDeniedHandlerTest {

    @Test
    void handle_shouldAnswer403AsOurProblem_withTheBearerChallenge() {
        ProblemAccessDeniedHandler handler =
                new ProblemAccessDeniedHandler(new ProblemResponseWriter(ServerCodecConfigurer.create()));
        MockServerWebExchange exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/chat-service/x"));

        StepVerifier.create(handler.handle(exchange, new AccessDeniedException("denied")))
                .expectComplete()
                .verify(Duration.ofSeconds(10));

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(exchange.getResponse().getHeaders().getFirst(HttpHeaders.WWW_AUTHENTICATE)).startsWith("Bearer");
        assertThat(exchange.getResponse().getHeaders().getContentType()).isEqualTo(MediaType.APPLICATION_PROBLEM_JSON);
        assertThat(exchange.getResponse().getBodyAsString().block(Duration.ofSeconds(10)))
                .contains("\"code\":\"CHT-REQ-002\"")
                .contains("\"requestId\":\"" + exchange.getRequest().getId() + "\"");
    }
}
