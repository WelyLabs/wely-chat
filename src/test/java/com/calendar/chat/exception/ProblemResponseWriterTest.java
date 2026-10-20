package com.calendar.chat.exception;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerCodecConfigurer;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.test.StepVerifier;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProblemResponseWriterTest {

    private final ProblemResponseWriter writer = new ProblemResponseWriter(ServerCodecConfigurer.create());

    @Test
    void write_shouldAnswerTheProblemsStatus_asProblemJson_withOurPropertiesAtTheTopLevel() {
        MockServerWebExchange exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/chat-service/x"));

        StepVerifier.create(writer.write(exchange, ProblemDetails.of(HttpStatus.BAD_GATEWAY, "CHT-TEC-002",
                        "Calls unavailable", "detail", exchange)))
                .expectComplete()
                .verify(Duration.ofSeconds(10));

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.BAD_GATEWAY);
        assertThat(exchange.getResponse().getHeaders().getContentType()).isEqualTo(MediaType.APPLICATION_PROBLEM_JSON);
        assertThat(exchange.getResponse().getBodyAsString().block(Duration.ofSeconds(10)))
                .contains("\"code\":\"CHT-TEC-002\"")
                .contains("\"requestId\":\"" + exchange.getRequest().getId() + "\"")
                .doesNotContain("\"properties\"");
    }

    @Test
    void constructor_shouldFail_whenNoCodecWritesProblemJson() {
        ServerCodecConfigurer withoutJackson = ServerCodecConfigurer.create();
        withoutJackson.registerDefaults(false);

        assertThatThrownBy(() -> new ProblemResponseWriter(withoutJackson))
                .isInstanceOf(IllegalStateException.class);
    }
}
