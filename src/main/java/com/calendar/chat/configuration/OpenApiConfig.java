package com.calendar.chat.configuration;

import com.calendar.chat.exception.BusinessErrorCode;
import com.calendar.chat.exception.GlobalErrorHandler;
import com.calendar.chat.exception.ProblemDetails;
import com.calendar.chat.exception.TechnicalErrorCode;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.DateTimeSchema;
import io.swagger.v3.oas.models.media.IntegerSchema;
import io.swagger.v3.oas.models.media.MapSchema;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.ObjectSchema;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.media.StringSchema;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;
import org.springdoc.core.customizers.OpenApiCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * Names the API that springdoc generates from the controllers, and documents how it fails.
 *
 * <p>Without this the document is titled after the application and says nothing about what the
 * service is for. The operations themselves are derived from the handler signatures, so what is
 * worth writing by hand is the part no signature carries — the error contract included: every
 * failure answers the same {@code application/problem+json} body, whose {@code code} is what a
 * client branches on.
 */
@Configuration
public class OpenApiConfig {

    static final String PROBLEM_SCHEMA = "Problem";
    static final String PROBLEM_MEDIA_TYPE = org.springframework.http.MediaType.APPLICATION_PROBLEM_JSON_VALUE;
    static final String CLIENT_ERRORS = "4XX";
    static final String SERVER_ERRORS = "5XX";

    private static final String CODE_DESCRIPTION = """
            Stable identifier a client branches on. The family tells what to do:
            BUS — a business rule refused the request: change the request;
            VAL — one or more fields are invalid, listed in `errors`;
            REQ — the framework refused the request (unknown path, method not allowed, unreadable body);
            TEC — a dependency failed: retry later, and it is an incident on our side.""";

    @Bean
    public OpenAPI openApi() {
        return new OpenAPI().info(new Info()
                .title("wely-chat")
                .version("v1")
                .description("Conversations and real-time messaging. REST for history, RSocket for the live stream, bucket pattern in MongoDB."));
    }

    /** Registers the {@code Problem} schema and gives every operation its 4XX and 5XX answers. */
    @Bean
    public OpenApiCustomizer problemResponses() {
        return openApi -> {
            if (openApi.getComponents() == null) {
                openApi.components(new Components());
            }
            openApi.getComponents().addSchemas(PROBLEM_SCHEMA, problemSchema());

            if (openApi.getPaths() == null) {
                return;
            }
            openApi.getPaths().values().forEach(path -> path.readOperations().forEach(operation -> {
                if (operation.getResponses() == null) {
                    operation.responses(new ApiResponses());
                }
                ApiResponses responses = operation.getResponses();
                responses.putIfAbsent(CLIENT_ERRORS, problemResponse(
                        "The request was refused: a business rule, an invalid field or the framework itself."));
                responses.putIfAbsent(SERVER_ERRORS, problemResponse(
                        "The request could not be completed: a dependency failed, or something nothing expected."));
            }));
        };
    }

    /** Every code this service can answer, read from the enums and the handler, so it cannot drift. */
    static List<String> allCodes() {
        return Stream.of(
                        Arrays.stream(BusinessErrorCode.values()).map(BusinessErrorCode::getCode),
                        Arrays.stream(TechnicalErrorCode.values()).map(TechnicalErrorCode::getCode),
                        GlobalErrorHandler.HANDLER_CODES.stream())
                .flatMap(codes -> codes)
                .distinct()
                .sorted()
                .toList();
    }

    private static Schema<?> problemSchema() {
        StringSchema code = new StringSchema();
        code.description(CODE_DESCRIPTION);
        code.setEnum(allCodes());

        return new ObjectSchema()
                .description("RFC 7807 problem detail, the body of every error this service answers.")
                .addProperty("type", new StringSchema().format("uri"))
                .addProperty("title", new StringSchema())
                .addProperty("status", new IntegerSchema())
                .addProperty("detail", new StringSchema())
                .addProperty("instance", new StringSchema().format("uri"))
                .addProperty(ProblemDetails.CODE_PROPERTY, code)
                .addProperty(ProblemDetails.TIMESTAMP_PROPERTY, new DateTimeSchema())
                .addProperty(ProblemDetails.REQUEST_ID_PROPERTY, new StringSchema()
                        .description("The WebFlux request id, as it appears in this service's logs."))
                .addProperty("errors", new MapSchema()
                        .additionalProperties(new StringSchema())
                        .description("Invalid fields and why, by field name. Only on "
                                + GlobalErrorHandler.VALIDATION_FAILED_CODE + "."));
    }

    private static ApiResponse problemResponse(String description) {
        return new ApiResponse()
                .description(description)
                .content(new Content().addMediaType(PROBLEM_MEDIA_TYPE, new MediaType()
                        .schema(new Schema<>().$ref("#/components/schemas/" + PROBLEM_SCHEMA))));
    }
}
