package com.calendar.chat.configuration;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.method.HandlerTypePredicate;
import org.springframework.web.reactive.config.PathMatchConfigurer;
import org.springframework.web.reactive.config.WebFluxConfigurer;

/**
 * Adds the service prefix to this service's own controllers, on top of Boot's WebFlux setup.
 *
 * <p>Not {@code @EnableWebFlux}: that annotation switches off all of Boot's WebFlux
 * auto-configuration, and with it every {@code spring.webflux.*} and {@code spring.codec.*}
 * property — {@code spring.webflux.multipart.max-in-memory-size} was ignored, so a photo above
 * 256 KB was spilled to disk instead of held in memory. Boot still applies this configurer.
 */
@Configuration
public class WebConfig implements WebFluxConfigurer {

    @Override
    public void configurePathMatching(PathMatchConfigurer configurer) {
        // Scoped to this service's own controllers by package, not by the @RestController
        // annotation: that predicate also matched springdoc's OpenApiWebfluxResource, which
        // moved the specification to /chat-service/v3/api-docs and left /v3/api-docs a 404.
        configurer.addPathPrefix(
                "/chat-service",
                HandlerTypePredicate.forBasePackage("com.calendar.chat.application.rest"));
    }
}