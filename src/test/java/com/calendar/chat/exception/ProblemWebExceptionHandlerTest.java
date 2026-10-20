package com.calendar.chat.exception;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerCodecConfigurer;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.WebFilter;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class ProblemWebExceptionHandlerTest {

    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    private final ProblemWebExceptionHandler handler = new ProblemWebExceptionHandler(
            new GlobalErrorHandler(new TechnicalErrorCounter(meterRegistry)), new ProblemResponseWriter(ServerCodecConfigurer.create()));

    private static MockServerWebExchange exchange() {
        return MockServerWebExchange.from(MockServerHttpRequest.get("/chat-service/x"));
    }

    @Test
    void handle_shouldAnswerATechnicalFailure_asOurProblem_andCountIt() {
        MockServerWebExchange exchange = exchange();

        StepVerifier.create(handler.handle(exchange, new TechnicalException(TechnicalErrorCode.STORE_UNAVAILABLE)))
                .expectComplete()
                .verify(Duration.ofSeconds(10));

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.BAD_GATEWAY);
        assertThat(exchange.getResponse().getHeaders().getContentType()).isEqualTo(MediaType.APPLICATION_PROBLEM_JSON);
        assertThat(exchange.getResponse().getBodyAsString().block(Duration.ofSeconds(10)))
                .contains("\"code\":\"CHT-TEC-004\"")
                .contains("\"requestId\":\"" + exchange.getRequest().getId() + "\"");
        assertThat(meterRegistry.get(GlobalErrorHandler.TECHNICAL_ERRORS_METRIC)
                .tag(GlobalErrorHandler.CODE_TAG, "CHT-TEC-004").counter().count()).isEqualTo(1);
    }

    @Test
    void handle_shouldKeepTheStatus_ofARefusedRequest() {
        MockServerWebExchange exchange = exchange();

        StepVerifier.create(handler.handle(exchange, new ResponseStatusException(HttpStatus.NOT_FOUND)))
                .expectComplete()
                .verify(Duration.ofSeconds(10));

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(exchange.getResponse().getBodyAsString().block(Duration.ofSeconds(10)))
                .contains("\"code\":\"" + GlobalErrorHandler.REQUEST_REFUSED_CODE + "\"");
    }

    @Test
    @DisplayName("a response already on the wire cannot be rewritten: the failure is passed on")
    void handle_shouldPassTheFailureOn_whenTheResponseIsCommitted() {
        MockServerWebExchange exchange = exchange();
        exchange.getResponse().setComplete().block(Duration.ofSeconds(10));
        IllegalStateException failure = new IllegalStateException("late");

        StepVerifier.create(handler.handle(exchange, failure))
                .expectErrorMatches(error -> error == failure)
                .verify(Duration.ofSeconds(10));
    }

    /** A failure raised in a WebFilter, which no controller advice sees, in the real application. */
    @Nested
    @SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
    @ActiveProfiles("test")
    class InTheApplication {

        static final String FAILING_PATH = "/chat-service/filter-failure";

        @TestConfiguration
        static class FailingFilter {

            // Ahead of the security chain, so the failure is the filter's, not a 401.
            @Bean
            @Order(Ordered.HIGHEST_PRECEDENCE)
            WebFilter failingFilter() {
                return (exchange, chain) -> exchange.getRequest().getPath().value().equals(FAILING_PATH)
                        ? Mono.error(new TechnicalException(TechnicalErrorCode.CALLS_UNAVAILABLE))
                        : chain.filter(exchange);
            }
        }

        @Value("${local.server.port}")
        private int port;

        @Autowired
        private MeterRegistry registry;

        @Test
        void handle_shouldAnswerAFilterFailure_asOurProblem_andCountIt() {
            WebTestClient.bindToServer().baseUrl("http://localhost:" + port)
                    .responseTimeout(Duration.ofSeconds(30))
                    .build()
                    .get().uri(FAILING_PATH)
                    .exchange()
                    .expectStatus().isEqualTo(502)
                    .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON)
                    .expectBody()
                    .jsonPath("$.code").isEqualTo("CHT-TEC-002")
                    .jsonPath("$.instance").isEqualTo(FAILING_PATH)
                    .jsonPath("$.requestId").value(id -> assertThat(id).asString().isNotBlank());

            assertThat(registry.get(GlobalErrorHandler.TECHNICAL_ERRORS_METRIC)
                    .tag(GlobalErrorHandler.CODE_TAG, "CHT-TEC-002").counter().count()).isEqualTo(1);
        }
    }
}
