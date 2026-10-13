package com.calendar.chat.configuration;

import com.calendar.chat.infrastructure.media.signing.GarageRequestSigner;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Clock;

/** The object storage, Garage in the cluster: an HTTP client and the signer for its S3 API. */
@Configuration
public class MediaClientConfig {

    /** No base URL: every request carries its own signed, absolute URI. */
    @Bean
    public WebClient mediaWebClient() {
        return WebClient.builder().build();
    }

    @Bean
    public GarageRequestSigner garageRequestSigner(@Value("${app.media.access-key}") String accessKey,
                                                   @Value("${app.media.secret-key}") String secretKey,
                                                   @Value("${app.media.region}") String region) {
        return new GarageRequestSigner(accessKey, secretKey, region, Clock.systemUTC());
    }
}
