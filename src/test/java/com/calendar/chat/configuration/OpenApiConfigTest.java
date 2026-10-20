package com.calendar.chat.configuration;

import com.calendar.chat.exception.BusinessErrorCode;
import com.calendar.chat.exception.GlobalErrorHandler;
import com.calendar.chat.exception.TechnicalErrorCode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.reactive.server.WebTestClient;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers {@link OpenApiConfig#openApi} and the two things that make the documentation useful:
 * that it answers without a token, and that it describes the paths the gateway actually routes.
 *
 * <p>The specification is reachable in-cluster and in dev only. The gateway forwards
 * {@code /api/v1/<service>/**} and nothing else, so none of these paths is exposed publicly.
 */
// A running server, not bindToApplicationContext: springdoc registers its routes on the web
// handler, and the context-bound client does not see them — the first version of this test 404ed
// for that reason, which masked the real defect the last test here guards against.
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
class OpenApiConfigTest {

    @Autowired
    private ApplicationContext context;

    @Value("${local.server.port}")
    private int port;

    private WebTestClient client() {
        return WebTestClient.bindToServer().baseUrl("http://localhost:" + port).build();
    }

    @Test
    @DisplayName("the generated document names the service rather than the application class")
    void openApi_shouldDescribeTheService() {
        var info = context.getBean(OpenAPI.class).getInfo();

        assertThat(info.getTitle()).isEqualTo("wely-chat");
        assertThat(info.getVersion()).isEqualTo("v1");
        assertThat(info.getDescription()).isNotBlank();
    }

    @Test
    @DisplayName("the specification answers without a token")
    void apiDocs_shouldBeReachableUnauthenticated() {
        client().get().uri("/v3/api-docs")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.info.title").isEqualTo("wely-chat");
    }

    @Test
    @DisplayName("the Swagger UI and its assets load without a token")
    void swaggerUi_shouldBeReachableUnauthenticated() {
        client().get().uri("/v3/api-docs/swagger-config")
                .exchange()
                .expectStatus().isOk();

        client().get().uri("/webjars/swagger-ui/index.html")
                .exchange()
                .expectStatus().isOk();
    }

    @Test
    @DisplayName("the documented paths carry the service prefix the gateway strips")
    void apiDocs_shouldDocumentPrefixedPaths() {
        // The regression this guards: WebConfig used to prefix every @RestController on the
        // classpath, springdoc's own included. That moved the specification itself to
        // /chat-service/v3/api-docs behind authentication, and /v3/api-docs answered 404.
        byte[] body = client().get().uri("/v3/api-docs")
                .exchange()
                .expectStatus().isOk()
                .expectBody().returnResult().getResponseBody();

        assertThat(body).isNotNull();
        assertThat(new String(body, java.nio.charset.StandardCharsets.UTF_8))
                .contains("\"/chat-service/");
    }

    @Test
    @DisplayName("the Problem schema lists every code, business and technical alike, read from the enums")
    void problemResponses_shouldRegisterTheProblemSchema_withEveryCode() {
        client().get().uri("/v3/api-docs")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.components.schemas.Problem.properties.requestId").exists()
                .jsonPath("$.components.schemas.Problem.properties.code.description").value(description ->
                        assertThat(description).asString().contains("BUS", "VAL", "REQ", "TEC"))
                .jsonPath("$.components.schemas.Problem.properties.code.enum").value(codes ->
                        assertThat((List<Object>) codes).contains(
                                BusinessErrorCode.CONVERSATION_NOT_FOUND.getCode(),
                                TechnicalErrorCode.STORE_UNAVAILABLE.getCode(),
                                GlobalErrorHandler.UNEXPECTED_FAILURE));
    }

    @Test
    @DisplayName("every documented operation answers its errors as application/problem+json")
    void problemResponses_shouldAddProblemResponses_toEveryOperation() {
        String problemRef = "#/components/schemas/" + OpenApiConfig.PROBLEM_SCHEMA;

        client().get().uri("/v3/api-docs")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.paths.*.*.responses['4XX'].content['application/problem+json'].schema['$ref']")
                .value(refs -> assertThat((List<Object>) refs).isNotEmpty().containsOnly(problemRef))
                .jsonPath("$.paths.*.*.responses['5XX'].content['application/problem+json'].schema['$ref']")
                .value(refs -> assertThat((List<Object>) refs).isNotEmpty().containsOnly(problemRef));
    }

    @Test
    @DisplayName("a 4XX or 5XX an operation already declares is kept as written")
    void problemResponses_shouldKeepAnErrorResponseAlreadyDeclared() {
        ApiResponse declared = new ApiResponse().description("declared by hand");
        Operation operation = new Operation().responses(new ApiResponses().addApiResponse(OpenApiConfig.CLIENT_ERRORS, declared));
        OpenAPI document = new OpenAPI().paths(new Paths().addPathItem("/x", new PathItem().get(operation)));

        new OpenApiConfig().problemResponses().customise(document);

        assertThat(operation.getResponses().get(OpenApiConfig.CLIENT_ERRORS)).isSameAs(declared);
        assertThat(operation.getResponses().get(OpenApiConfig.SERVER_ERRORS).getContent())
                .containsKey(OpenApiConfig.PROBLEM_MEDIA_TYPE);
        assertThat(document.getComponents().getSchemas()).containsKey(OpenApiConfig.PROBLEM_SCHEMA);
    }

    @Test
    void allCodes_shouldListTheEnumsAndTheHandlerCodes_once() {
        assertThat(OpenApiConfig.allCodes())
                .contains(BusinessErrorCode.FRIENDSHIP_REQUIRED.getCode(), TechnicalErrorCode.CALLS_UNAVAILABLE.getCode())
                .containsAll(GlobalErrorHandler.HANDLER_CODES)
                .doesNotHaveDuplicates()
                .hasSize(BusinessErrorCode.values().length + TechnicalErrorCode.values().length
                        + GlobalErrorHandler.HANDLER_CODES.size());
    }
}
