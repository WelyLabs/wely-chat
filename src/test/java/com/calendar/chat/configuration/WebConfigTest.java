package com.calendar.chat.configuration;

import com.calendar.chat.application.rest.ConversationController;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.http.codec.ServerCodecConfigurer;
import org.springframework.http.codec.multipart.DefaultPartHttpMessageReader;
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.util.unit.DataSize;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.method.HandlerTypePredicate;
import org.springframework.web.reactive.config.PathMatchConfigurer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class WebConfigTest {

    private final WebConfig config = new WebConfig();

    @Test
    @DisplayName("controllers are prefixed with the service segment")
    void configurePathMatching_shouldPrefixControllersWithTheServiceSegment() {
        PathMatchConfigurer configurer = mock(PathMatchConfigurer.class);

        config.configurePathMatching(configurer);

        // The gateway strips /api/v1 from /api/v1/chat-service/** and leaves
        // /chat-service, which this prefix reattaches: a service answers on the same
        // path whether it is called through the gateway or directly.
        verify(configurer).addPathPrefix(eq("/chat-service"), any(HandlerTypePredicate.class));
    }

    @Test
    @DisplayName("the prefix applies to this service's own controllers only")
    void configurePathMatching_shouldTargetThisServicesControllersOnly() {
        PathMatchConfigurer configurer = mock(PathMatchConfigurer.class);
        ArgumentCaptor<HandlerTypePredicate> predicate =
                ArgumentCaptor.forClass(HandlerTypePredicate.class);

        config.configurePathMatching(configurer);
        verify(configurer).addPathPrefix(eq("/chat-service"), predicate.capture());

        assertThat(predicate.getValue().test(ConversationController.class)).isTrue();

        // Selecting on the @RestController annotation instead would also match springdoc's
        // OpenApiWebfluxResource, which served the specification at
        // /chat-service/v3/api-docs — behind authentication — and left /v3/api-docs a 404.
        // ForeignController stands in for it: annotated, but not ours.
        assertThat(predicate.getValue().test(ForeignController.class)).isFalse();
        assertThat(predicate.getValue().test(PlainClass.class)).isFalse();
    }

    @RestController
    private static class ForeignController { }

    private static class PlainClass { }

    /** Boot's WebFlux auto-configuration, which {@code @EnableWebFlux} used to switch off, in the real application. */
    @Nested
    @SpringBootTest
    @ActiveProfiles("test")
    class InTheApplication {

        @MockitoBean
        private ReactiveJwtDecoder jwtDecoder;

        @Autowired
        private ServerCodecConfigurer codecs;

        @Autowired
        private Environment environment;

        @Test
        @DisplayName("spring.webflux.multipart.max-in-memory-size reaches the multipart reader: a photo stays in memory")
        void codecs_shouldApplyTheMultipartInMemoryLimit() {
            long configured = DataSize.parse(environment.getRequiredProperty("spring.webflux.multipart.max-in-memory-size"))
                    .toBytes();

            assertThat(configured).isEqualTo(DataSize.ofMegabytes(11).toBytes());
            assertThat(codecs.getReaders())
                    .filteredOn(DefaultPartHttpMessageReader.class::isInstance)
                    .singleElement()
                    .satisfies(reader -> assertThat(((DefaultPartHttpMessageReader) reader).getMaxInMemorySize())
                            .isEqualTo(configured));
        }
    }
}
