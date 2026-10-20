package com.calendar.chat.configuration;

import com.calendar.chat.exception.RequestIds;
import org.springframework.boot.rsocket.messaging.RSocketStrategiesCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Reads a client's request id from RSocket metadata ({@code message/x.wely.request-id}, text), so an
 * error frame and the log line it comes with quote the id the client already has.
 */
@Configuration
public class RSocketMetadataConfig {

    @Bean
    public RSocketStrategiesCustomizer requestIdMetadata() {
        return strategies -> strategies.metadataExtractorRegistry(registry -> registry.metadataToExtract(
                RequestIds.REQUEST_ID_MIME_TYPE, String.class, RequestIds.REQUEST_ID_ATTRIBUTE));
    }
}
