package com.calendar.chat.configuration;

import com.calendar.chat.exception.ProblemAccessDeniedHandler;
import com.calendar.chat.exception.ProblemAuthenticationEntryPoint;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.jwt.*;
import org.springframework.security.web.server.SecurityWebFilterChain;

@Configuration
@EnableWebFluxSecurity
public class WebFluxSecurityConfig {

    @Value("${spring.security.oauth2.resourceserver.jwt.issuer-uri}")
    private String publicIssuer;

    @Value("${spring.security.oauth2.resourceserver.jwt.jwk-set-uri}")
    private String internalJwkSetUri;

    @Bean
    public SecurityWebFilterChain apiHttpSecurity(ServerHttpSecurity http,
                                                  ProblemAuthenticationEntryPoint authenticationEntryPoint,
                                                  ProblemAccessDeniedHandler accessDeniedHandler) {
        return http
                .cors(Customizer.withDefaults())
                .csrf(ServerHttpSecurity.CsrfSpec::disable)
                .authorizeExchange((authorize) -> authorize
                        .pathMatchers(HttpMethod.OPTIONS, "/**").permitAll()
                        // Kubernetes probes: the kubelet carries no token. Only the two
                        // health groups are opened, not /actuator as a whole.
                        .pathMatchers("/actuator/health/liveness", "/actuator/health/readiness").permitAll()
                        // Prometheus scrapes without a token. The Service is ClusterIP only and the
                        // gateway never routes /actuator/** (it forwards /api/v1/<service>/** to
                        // /<service>/**), so this is reachable in-cluster only; and the metrics hold
                        // counters, no user data. Only this endpoint, not /actuator as a whole.
                        .pathMatchers(HttpMethod.GET, "/actuator/prometheus").permitAll()
                        // OpenAPI. These paths are not routed by the gateway — it forwards
                        // /api/v1/<service>/** and /rsocket only — so opening them here makes
                        // the documentation reachable in-cluster and in dev, and nowhere else.
                        .pathMatchers("/v3/api-docs", "/v3/api-docs/**", "/swagger-ui/**",
                                      "/swagger-ui.html", "/webjars/swagger-ui/**").permitAll()
                        .anyExchange().authenticated())
                // A refused request answers this service's problem, as every other error does.
                .exceptionHandling(errors -> errors
                        .authenticationEntryPoint(authenticationEntryPoint)
                        .accessDeniedHandler(accessDeniedHandler))
                .oauth2ResourceServer(oauth2 -> oauth2
                        .authenticationEntryPoint(authenticationEntryPoint)
                        .accessDeniedHandler(accessDeniedHandler)
                        .jwt(Customizer.withDefaults()))
                .build();
    }

    @Bean
    public ReactiveJwtDecoder jwtDecoder() {
        NimbusReactiveJwtDecoder jwtDecoder = NimbusReactiveJwtDecoder.withJwkSetUri(internalJwkSetUri).build();

        OAuth2TokenValidator<Jwt> issuerValidator = JwtValidators.createDefaultWithIssuer(publicIssuer);
        OAuth2TokenValidator<Jwt> withTimestamp = new JwtTimestampValidator();
        OAuth2TokenValidator<Jwt> validator = new DelegatingOAuth2TokenValidator<>(withTimestamp, issuerValidator);

        jwtDecoder.setJwtValidator(validator);
        return jwtDecoder;
    }
}
