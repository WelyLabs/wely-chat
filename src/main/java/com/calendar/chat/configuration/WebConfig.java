package com.calendar.chat.configuration;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.method.HandlerTypePredicate;
import org.springframework.web.reactive.config.EnableWebFlux;
import org.springframework.web.reactive.config.PathMatchConfigurer;
import org.springframework.web.reactive.config.WebFluxConfigurer;

@Configuration
@EnableWebFlux
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