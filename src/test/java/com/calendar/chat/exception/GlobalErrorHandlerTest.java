package com.calendar.chat.exception;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.core.MethodParameter;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.validation.BeanPropertyBindingResult;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.support.WebExchangeBindException;
import org.springframework.web.reactive.result.method.annotation.ResponseEntityExceptionHandler;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

class GlobalErrorHandlerTest {

    private SimpleMeterRegistry meterRegistry;
    private WebTestClient client;

    @RestController
    static class TestController {

        @GetMapping("/not-found")
        Mono<Void> notFound() {
            return Mono.error(new BusinessException(BusinessErrorCode.CONVERSATION_NOT_FOUND));
        }

        @GetMapping("/forbidden")
        Mono<Void> forbidden() {
            return Mono.error(new BusinessException(BusinessErrorCode.MESSAGE_PAGE_NOT_FOUND));
        }

        @GetMapping("/generic-error")
        Mono<Void> genericError() {
            return Mono.error(new IllegalStateException("mongodb://admin:hunter2@host/events"));
        }

        @GetMapping("/account-deleted")
        Mono<Void> accountDeleted() {
            return Mono.error(new BusinessException(BusinessErrorCode.ACCOUNT_DELETED));
        }

        @GetMapping("/read-only")
        Mono<Void> readOnly() {
            return Mono.error(new BusinessException(BusinessErrorCode.CONVERSATION_READ_ONLY));
        }

        @GetMapping("/store-down")
        Mono<Void> storeDown() {
            return Mono.error(new TechnicalException(TechnicalErrorCode.STORE_UNAVAILABLE));
        }

        @GetMapping("/calls-down")
        Mono<Void> callsDown() {
            return Mono.error(new TechnicalException(TechnicalErrorCode.CALLS_UNAVAILABLE));
        }

        @GetMapping("/gone")
        Mono<Void> gone() {
            return Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND));
        }

        @GetMapping("/not-allowed")
        Mono<Void> notAllowed() {
            return Mono.error(new ResponseStatusException(HttpStatus.METHOD_NOT_ALLOWED));
        }

        @GetMapping("/invalid")
        Mono<Void> invalid() throws NoSuchMethodException {
            BeanPropertyBindingResult result = new BeanPropertyBindingResult(new Object(), "request");
            result.addError(new FieldError("request", "content", "must not be blank"));
            MethodParameter parameter = new MethodParameter(TestController.class.getDeclaredMethod("invalid"), -1);
            return Mono.error(new WebExchangeBindException(parameter, result));
        }
    }

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        client = WebTestClient.bindToController(new TestController())
                .controllerAdvice(new GlobalErrorHandler(new TechnicalErrorCounter(meterRegistry)))
                .build();
    }

    private double technicalErrors(String code) {
        var counter = meterRegistry.find(GlobalErrorHandler.TECHNICAL_ERRORS_METRIC).tag(GlobalErrorHandler.CODE_TAG, code).counter();
        return counter == null ? 0 : counter.count();
    }

    private void assertCarriesARequestId(String uri) {
        client.get().uri(uri)
                .exchange()
                .expectBody()
                .jsonPath("$.requestId").value(id -> assertThat(id).asString().isNotBlank());
    }

    @Test
    @DisplayName("a business failure answers as application/problem+json")
    void handleBusinessException_shouldAnswerAsProblemDetail() {
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
    void handleBusinessException_shouldDistinguishCodesAtTheSameStatus() {
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
                .jsonPath("$.code").isEqualTo(GlobalErrorHandler.UNEXPECTED_FAILURE_CODE)
                .jsonPath("$.detail").value(detail -> {
                    // The message carried a Mongo URI with a password.
                    if (detail.toString().contains("hunter2") || detail.toString().contains("mongodb")) {
                        throw new AssertionError("the original message leaked into the response");
                    }
                });
    }

    @Test
    @DisplayName("an unknown path keeps its 404 instead of becoming a 500")
    void handleResponseStatusException_shouldKeepTheOriginalStatus() {
        // The regression this guards: @ExceptionHandler(Exception.class) also catches
        // ResponseStatusException, so before CHT-REQ-000 existed every unknown path answered
        // 500. The service reported a fault of its own for a request it had handled correctly.
        client.get().uri("/gone")
                .exchange()
                .expectStatus().isNotFound()
                .expectHeader().contentType(MediaType.APPLICATION_PROBLEM_JSON)
                .expectBody()
                .jsonPath("$.status").isEqualTo(404)
                .jsonPath("$.title").isEqualTo("Not Found")
                .jsonPath("$.code").isEqualTo(GlobalErrorHandler.REQUEST_REFUSED_CODE);
    }

    @Test
    @DisplayName("a rejected method keeps its 405")
    void handleResponseStatusException_shouldCarryAnyStatusThrough() {
        client.get().uri("/not-allowed")
                .exchange()
                .expectStatus().isEqualTo(405)
                .expectBody()
                .jsonPath("$.status").isEqualTo(405)
                .jsonPath("$.title").isEqualTo("Method Not Allowed")
                .jsonPath("$.code").isEqualTo(GlobalErrorHandler.REQUEST_REFUSED_CODE);
    }

    @Test
    @DisplayName("a deleted account answers 410, so the client knows retrying cannot help")
    void handleBusinessException_shouldAnswer410WithItsCode_forAccountDeleted() {
        client.get().uri("/account-deleted")
                .exchange()
                .expectStatus().isEqualTo(410)
                .expectBody()
                .jsonPath("$.code").isEqualTo("CHT-BUS-011")
                .jsonPath("$.title").isEqualTo("Account deleted");
    }

    @Test
    void handleBusinessException_shouldAnswer409WithItsCode_forReadOnlyConversation() {
        client.get().uri("/read-only")
                .exchange()
                .expectStatus().isEqualTo(409)
                .expectBody()
                .jsonPath("$.code").isEqualTo("CHT-BUS-012");
    }

    @Test
    @DisplayName("a store that cannot be reached is a dependency down, 502, not a fault of ours")
    void handleTechnicalException_shouldAnswer502WithItsCode_forStoreUnavailable() {
        client.get().uri("/store-down")
                .exchange()
                .expectStatus().isEqualTo(502)
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON)
                .expectBody()
                .jsonPath("$.status").isEqualTo(502)
                .jsonPath("$.code").isEqualTo("CHT-TEC-004")
                .jsonPath("$.title").isEqualTo("Message store unavailable")
                .jsonPath("$.detail").isEqualTo("The message store could not be reached; try again shortly.")
                .jsonPath("$.type").isEqualTo("https://welylabs.app/problems/cht-tec-004")
                .jsonPath("$.instance").isEqualTo("/store-down")
                .jsonPath("$.timestamp").exists();
    }

    @Test
    @DisplayName("each dependency keeps its own code, so an incident is routed without reading the logs")
    void handleTechnicalException_shouldDistinguishDependenciesAtTheSameStatus() {
        client.get().uri("/calls-down")
                .exchange()
                .expectStatus().isEqualTo(502)
                .expectBody()
                .jsonPath("$.code").isEqualTo("CHT-TEC-002")
                .jsonPath("$.title").isEqualTo("Calls unavailable")
                .jsonPath("$.detail").isEqualTo("The call service could not be reached; try again shortly.");
    }

    @Test
    @DisplayName("every answer carries the request id that prefixes the framework's log lines")
    void problem_shouldCarryTheRequestId_whateverTheHandler() {
        assertCarriesARequestId("/not-found");
        assertCarriesARequestId("/store-down");
        assertCarriesARequestId("/invalid");
        assertCarriesARequestId("/gone");
        assertCarriesARequestId("/generic-error");
    }

    @Test
    void handleValidationException_shouldAnswer400WithTheOffendingFields() {
        client.get().uri("/invalid")
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.code").isEqualTo(GlobalErrorHandler.VALIDATION_FAILED_CODE)
                .jsonPath("$.errors.content").isEqualTo("must not be blank");

        assertThat(technicalErrors(GlobalErrorHandler.VALIDATION_FAILED_CODE)).isZero();
    }

    @Test
    @DisplayName("a technical failure is counted under its own code")
    void handleTechnicalException_shouldCountTheFailure_byCode() {
        client.get().uri("/store-down").exchange().expectStatus().isEqualTo(502);
        client.get().uri("/store-down").exchange().expectStatus().isEqualTo(502);
        client.get().uri("/calls-down").exchange().expectStatus().isEqualTo(502);

        assertThat(technicalErrors("CHT-TEC-004")).isEqualTo(2);
        assertThat(technicalErrors("CHT-TEC-002")).isEqualTo(1);
        assertThat(meterRegistry.find(GlobalErrorHandler.TECHNICAL_ERRORS_METRIC).counters())
                .allSatisfy(counter -> assertThat(counter.getId().getTag(GlobalErrorHandler.SOURCE_TAG))
                        .isEqualTo(GlobalErrorHandler.SOURCE_HTTP));
    }

    @Test
    @DisplayName("an unexpected failure is counted as a technical one")
    void handleUnexpectedException_shouldCountTheFailure() {
        client.get().uri("/generic-error").exchange().expectStatus().is5xxServerError();

        assertThat(technicalErrors(GlobalErrorHandler.UNEXPECTED_FAILURE_CODE)).isEqualTo(1);
    }

    @Test
    @DisplayName("a business failure or a refused request is not an incident, so it is not counted")
    void handleBusinessException_shouldNotCountAnything() {
        client.get().uri("/not-found").exchange().expectStatus().isNotFound();
        client.get().uri("/gone").exchange().expectStatus().isNotFound();

        assertThat(meterRegistry.find(GlobalErrorHandler.TECHNICAL_ERRORS_METRIC).counters()).isEmpty();
    }

    private static MockServerWebExchange exchange() {
        return MockServerWebExchange.from(MockServerHttpRequest.get("/chat-service/x"));
    }

    @Test
    @DisplayName("handle maps each kind of failure exactly as the matching @ExceptionHandler does")
    void handle_shouldMapEachFailure_likeItsExceptionHandler() {
        GlobalErrorHandler handler = new GlobalErrorHandler(new TechnicalErrorCounter(meterRegistry));

        assertThat(handler.handle(new BusinessException(BusinessErrorCode.CALL_FULL), exchange()).getProperties())
                .containsEntry(ProblemDetails.CODE_PROPERTY, "CHT-BUS-009");
        assertThat(handler.handle(new TechnicalException(TechnicalErrorCode.MEDIA_UNAVAILABLE), exchange()).getStatus())
                .isEqualTo(502);
        assertThat(handler.handle(new ResponseStatusException(HttpStatus.NOT_FOUND), exchange()).getProperties())
                .containsEntry(ProblemDetails.CODE_PROPERTY, GlobalErrorHandler.REQUEST_REFUSED_CODE);
        assertThat(handler.handle(new IllegalStateException("boom"), exchange()).getProperties())
                .containsEntry(ProblemDetails.CODE_PROPERTY, GlobalErrorHandler.UNEXPECTED_FAILURE_CODE);

        assertThat(technicalErrors("CHT-TEC-001")).isEqualTo(1);
        assertThat(technicalErrors(GlobalErrorHandler.UNEXPECTED_FAILURE_CODE)).isEqualTo(1);
    }

    @Test
    @DisplayName("an Error, not an Exception, is still answered and counted as unexpected")
    void handle_shouldAnswerAnError_asUnexpected() {
        ProblemDetail problem = new GlobalErrorHandler(new TechnicalErrorCounter(meterRegistry)).handle(new AssertionError("boom"), exchange());

        assertThat(problem.getStatus()).isEqualTo(500);
        assertThat(problem.getProperties()).containsEntry(ProblemDetails.CODE_PROPERTY,
                GlobalErrorHandler.UNEXPECTED_FAILURE_CODE);
        assertThat(technicalErrors(GlobalErrorHandler.UNEXPECTED_FAILURE_CODE)).isEqualTo(1);
    }

    @Test
    void handle_shouldListTheOffendingFields_forAValidationFailure() throws NoSuchMethodException {
        BeanPropertyBindingResult result = new BeanPropertyBindingResult(new Object(), "request");
        result.addError(new FieldError("request", "content", "must not be blank"));
        MethodParameter parameter = new MethodParameter(TestController.class.getDeclaredMethod("invalid"), -1);

        ProblemDetail problem = new GlobalErrorHandler(new TechnicalErrorCounter(meterRegistry))
                .handle(new WebExchangeBindException(parameter, result), exchange());

        assertThat(problem.getProperties()).containsEntry(ProblemDetails.CODE_PROPERTY,
                GlobalErrorHandler.VALIDATION_FAILED_CODE);
        assertThat(problem.getProperties().get("errors")).isEqualTo(java.util.Map.of("content", "must not be blank"));
    }

    /**
     * The same failures in the real application: every filter, every exception handler and the
     * framework's own advice are there, which a controller bound alone cannot show. Errors the
     * framework raises before or around a controller must still answer this service's problem.
     */
    @Nested
    @SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
    @ActiveProfiles("test")
    class InTheApplication {

        private static final String TOKEN = "test-token";

        @Value("${local.server.port}")
        private int port;

        @MockitoBean
        private ReactiveJwtDecoder jwtDecoder;

        @Autowired
        private ApplicationContext context;

        private WebTestClient server() {
            Jwt jwt = Jwt.withTokenValue(TOKEN).header("alg", "none").claim("businessId", "user-1").build();
            when(jwtDecoder.decode(anyString())).thenReturn(Mono.just(jwt));
            return WebTestClient.bindToServer()
                    .baseUrl("http://localhost:" + port)
                    .responseTimeout(Duration.ofSeconds(30))
                    .defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + TOKEN)
                    .build();
        }

        private void assertOurProblem(WebTestClient.ResponseSpec response, int status, String code) {
            response.expectStatus().isEqualTo(status)
                    .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON)
                    .expectBody()
                    .jsonPath("$.status").isEqualTo(status)
                    .jsonPath("$.code").isEqualTo(code)
                    .jsonPath("$.requestId").value(id -> assertThat(id).asString().isNotBlank());
        }

        @Test
        @DisplayName("Boot's ProblemDetailsExceptionHandler is not registered: spring.webflux.problemdetails.enabled stays unset")
        void context_shouldHoldNoFrameworkProblemAdvice() {
            assertThat(context.getBeansOfType(ResponseEntityExceptionHandler.class)).isEmpty();
        }

        @Test
        @DisplayName("the gateway's X-Request-Id is the problem's requestId, and echoed on the response")
        void problem_shouldQuoteTheGatewaysRequestId() {
            String gatewayId = "abcd1234-0000-4000-8000-000000000001";

            server().get().uri("/chat-service/no-such-path")
                    .header(RequestIds.REQUEST_ID_HEADER, gatewayId)
                    .exchange()
                    .expectStatus().isNotFound()
                    .expectHeader().valueEquals(RequestIds.REQUEST_ID_HEADER, gatewayId)
                    .expectBody()
                    .jsonPath("$.requestId").isEqualTo(gatewayId);
        }

        @Test
        @DisplayName("a refusal by the security chain quotes it too: the id is adopted before authentication")
        void authenticationRefusal_shouldQuoteTheGatewaysRequestId() {
            String gatewayId = "abcd1234-0000-4000-8000-000000000002";

            WebTestClient.bindToServer().baseUrl("http://localhost:" + port)
                    .responseTimeout(Duration.ofSeconds(30)).build()
                    .get().uri("/chat-service/conversations/all")
                    .header(RequestIds.REQUEST_ID_HEADER, gatewayId)
                    .exchange()
                    .expectStatus().isUnauthorized()
                    .expectBody()
                    .jsonPath("$.code").isEqualTo(BusinessErrorCode.AUTHENTICATION_REQUIRED.getCode())
                    .jsonPath("$.requestId").isEqualTo(gatewayId);
        }

        @Test
        void unknownPath_shouldAnswer404AsOurProblem() {
            assertOurProblem(server().get().uri("/chat-service/no-such-path").exchange(),
                    404, GlobalErrorHandler.REQUEST_REFUSED_CODE);
        }

        @Test
        void wrongMethod_shouldAnswer405AsOurProblem() {
            assertOurProblem(server().delete().uri("/chat-service/conversations/all").exchange(),
                    405, GlobalErrorHandler.REQUEST_REFUSED_CODE);
        }

        @Test
        void unsupportedContentType_shouldAnswer415AsOurProblem() {
            assertOurProblem(server().post().uri("/chat-service/calls")
                            .contentType(MediaType.TEXT_PLAIN).bodyValue("conversation").exchange(),
                    415, GlobalErrorHandler.REQUEST_REFUSED_CODE);
        }

        @Test
        void unreadableBody_shouldAnswer400AsOurProblem() {
            assertOurProblem(server().post().uri("/chat-service/calls")
                            .contentType(MediaType.APPLICATION_JSON).bodyValue("{not json").exchange(),
                    400, GlobalErrorHandler.REQUEST_REFUSED_CODE);
        }

        @Test
        @DisplayName("a parameter of the wrong type is an invalid field: 400")
        void invalidField_shouldAnswer400AsOurProblem() {
            assertOurProblem(server().get().uri("/chat-service/conversations/conv-1/loadMessages?bucketIndex=abc")
                            .exchange(),
                    400, GlobalErrorHandler.REQUEST_REFUSED_CODE);
        }
    }
}
