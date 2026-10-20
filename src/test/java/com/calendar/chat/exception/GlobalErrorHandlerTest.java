package com.calendar.chat.exception;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.MethodParameter;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.validation.BeanPropertyBindingResult;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.support.WebExchangeBindException;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;

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
                .controllerAdvice(new GlobalErrorHandler(meterRegistry))
                .build();
    }

    private double technicalErrors(String code) {
        var counter = meterRegistry.find(GlobalErrorHandler.TECHNICAL_ERRORS).tag(GlobalErrorHandler.CODE_TAG, code).counter();
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
                .jsonPath("$.code").isEqualTo(GlobalErrorHandler.UNEXPECTED_FAILURE)
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
                .jsonPath("$.code").isEqualTo(GlobalErrorHandler.REQUEST_REFUSED);
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
                .jsonPath("$.code").isEqualTo(GlobalErrorHandler.REQUEST_REFUSED);
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
                .jsonPath("$.code").isEqualTo(GlobalErrorHandler.VALIDATION_FAILED)
                .jsonPath("$.errors.content").isEqualTo("must not be blank");

        assertThat(technicalErrors(GlobalErrorHandler.VALIDATION_FAILED)).isZero();
    }

    @Test
    @DisplayName("a technical failure is counted under its own code")
    void handleTechnicalException_shouldCountTheFailure_byCode() {
        client.get().uri("/store-down").exchange().expectStatus().isEqualTo(502);
        client.get().uri("/store-down").exchange().expectStatus().isEqualTo(502);
        client.get().uri("/calls-down").exchange().expectStatus().isEqualTo(502);

        assertThat(technicalErrors("CHT-TEC-004")).isEqualTo(2);
        assertThat(technicalErrors("CHT-TEC-002")).isEqualTo(1);
    }

    @Test
    @DisplayName("an unexpected failure is counted as a technical one")
    void handleUnexpectedException_shouldCountTheFailure() {
        client.get().uri("/generic-error").exchange().expectStatus().is5xxServerError();

        assertThat(technicalErrors(GlobalErrorHandler.UNEXPECTED_FAILURE)).isEqualTo(1);
    }

    @Test
    @DisplayName("a business failure or a refused request is not an incident, so it is not counted")
    void handleBusinessException_shouldNotCountAnything() {
        client.get().uri("/not-found").exchange().expectStatus().isNotFound();
        client.get().uri("/gone").exchange().expectStatus().isNotFound();

        assertThat(meterRegistry.find(GlobalErrorHandler.TECHNICAL_ERRORS).counters()).isEmpty();
    }
}
