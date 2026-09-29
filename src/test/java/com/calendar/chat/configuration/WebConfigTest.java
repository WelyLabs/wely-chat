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
    @DisplayName("les contrôleurs sont préfixés par le segment de service")
    void configurePathMatching_shouldPrefixControllersWithTheServiceSegment() {
        PathMatchConfigurer configurer = mock(PathMatchConfigurer.class);

        config.configurePathMatching(configurer);

        // La gateway fait stripPrefix(2) sur /api/v1/chat-service/** et laisse /chat-service,
        // que ce préfixe réattache : un service répond donc sur le même chemin qu'il
        // soit appelé via la gateway ou directement.
        verify(configurer).addPathPrefix(eq("/chat-service"), any(HandlerTypePredicate.class));
    }

    @Test
    @DisplayName("le préfixe ne s'applique qu'aux @RestController")
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
