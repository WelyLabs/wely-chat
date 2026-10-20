package com.calendar.chat.exception;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;

class ProblemDetailsTest {

    private final MockServerWebExchange exchange =
            MockServerWebExchange.from(MockServerHttpRequest.get("/chat-service/conversations/c1"));

    @Test
    void of_shouldFillTheStandardFieldsAndOurs() {
        ProblemDetail problem = ProblemDetails.of(HttpStatus.BAD_GATEWAY, "CHT-TEC-004", "Message store unavailable",
                "The message store could not be reached; try again shortly.", exchange);

        assertThat(problem.getStatus()).isEqualTo(502);
        assertThat(problem.getType()).isEqualTo(URI.create("https://welylabs.app/problems/cht-tec-004"));
        assertThat(problem.getTitle()).isEqualTo("Message store unavailable");
        assertThat(problem.getDetail()).isEqualTo("The message store could not be reached; try again shortly.");
        assertThat(problem.getInstance()).isEqualTo(URI.create("/chat-service/conversations/c1"));
        assertThat(problem.getProperties())
                .containsEntry(ProblemDetails.CODE_PROPERTY, "CHT-TEC-004")
                .containsKey(ProblemDetails.TIMESTAMP_PROPERTY);
    }

    @Test
    void of_shouldCarryTheRequestId_ofTheExchange() {
        ProblemDetail problem = ProblemDetails.of(HttpStatus.NOT_FOUND, "CHT-REQ-000", "Not Found", "x", exchange);

        assertThat(problem.getProperties())
                .containsEntry(ProblemDetails.REQUEST_ID_PROPERTY, exchange.getRequest().getId());
        assertThat(exchange.getRequest().getId()).isNotBlank();
    }

    @Test
    void of_shouldTakeEverythingFromABusinessCode() {
        ProblemDetail problem = ProblemDetails.of(BusinessErrorCode.AUTHENTICATION_REQUIRED, exchange);

        assertThat(problem.getStatus()).isEqualTo(401);
        assertThat(problem.getTitle()).isEqualTo(BusinessErrorCode.AUTHENTICATION_REQUIRED.getTitle());
        assertThat(problem.getDetail()).isEqualTo(BusinessErrorCode.AUTHENTICATION_REQUIRED.getDetail());
        assertThat(problem.getProperties()).containsEntry(ProblemDetails.CODE_PROPERTY, "CHT-REQ-001");
    }
}
