package com.calendar.chat.configuration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
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
    @DisplayName("the prefix applies to @RestController only")
    void configurePathMatching_shouldTargetRestControllersOnly() {
        PathMatchConfigurer configurer = mock(PathMatchConfigurer.class);
        ArgumentCaptor<HandlerTypePredicate> predicate =
                ArgumentCaptor.forClass(HandlerTypePredicate.class);

        config.configurePathMatching(configurer);
        verify(configurer).addPathPrefix(eq("/chat-service"), predicate.capture());

        assertThat(predicate.getValue().test(AnnotatedController.class)).isTrue();
        assertThat(predicate.getValue().test(PlainClass.class)).isFalse();
    }

    @RestController
    private static class AnnotatedController { }

    private static class PlainClass { }
}
