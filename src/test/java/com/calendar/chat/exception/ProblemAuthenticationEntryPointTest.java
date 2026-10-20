package com.calendar.chat.exception;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerCodecConfigurer;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.security.authentication.AuthenticationCredentialsNotFoundException;
import org.springframework.security.oauth2.server.resource.InvalidBearerTokenException;
import reactor.test.StepVerifier;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class ProblemAuthenticationEntryPointTest {

    private final ProblemAuthenticationEntryPoint entryPoint =
            new ProblemAuthenticationEntryPoint(new ProblemResponseWriter(ServerCodecConfigurer.create()));

    private final MockServerWebExchange exchange =
            MockServerWebExchange.from(MockServerHttpRequest.get("/chat-service/conversations/all"));

    @Test
    void commence_shouldAnswer401AsOurProblem_withTheBearerChallenge() {
        StepVerifier.create(entryPoint.commence(exchange, new AuthenticationCredentialsNotFoundException("no token")))
                .expectComplete()
                .verify(Duration.ofSeconds(10));

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(exchange.getResponse().getHeaders().getFirst(HttpHeaders.WWW_AUTHENTICATE)).isEqualTo("Bearer");
        assertThat(exchange.getResponse().getHeaders().getContentType()).isEqualTo(MediaType.APPLICATION_PROBLEM_JSON);
        assertThat(exchange.getResponse().getBodyAsString().block(Duration.ofSeconds(10)))
                .contains("\"code\":\"CHT-REQ-001\"")
                .contains("\"requestId\":\"" + exchange.getRequest().getId() + "\"");
    }

    @Test
    void commence_shouldKeepTheReasonInTheChallenge_whenTheTokenIsInvalid() {
        StepVerifier.create(entryPoint.commence(exchange, new InvalidBearerTokenException("expired")))
                .expectComplete()
                .verify(Duration.ofSeconds(10));

        assertThat(exchange.getResponse().getHeaders().getFirst(HttpHeaders.WWW_AUTHENTICATE))
                .startsWith("Bearer").contains("error=\"invalid_token\"");
        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    void headersOnly_shouldNotCommitTheResponse_whenCompleted() {
        StepVerifier.create(ProblemAuthenticationEntryPoint.headersOnly(exchange).getResponse().setComplete())
                .expectComplete()
                .verify(Duration.ofSeconds(10));

        assertThat(exchange.getResponse().isCommitted()).isFalse();
    }
}
