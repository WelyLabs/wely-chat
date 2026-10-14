package com.calendar.chat.configuration;

import io.netty.channel.ChannelOption;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;

/**
 * Clients for Cloudflare Realtime: the SFU and the TURN credentials. Each authenticates with its
 * own secret, which never leaves this service. An empty secret still starts the application; the
 * calls then answer CALLS_UNAVAILABLE, or STUN only for the ICE servers.
 */
@Configuration
public class CallsClientConfig {

    /** A join or a publication waits on this; a stalled relay should fail fast. */
    static final Duration TIMEOUT = Duration.ofSeconds(5);
    static final String BEARER = "Bearer ";

    @Bean
    public WebClient realtimeWebClient(@Value("${app.calls.realtime.url}") String url,
                                       @Value("${app.calls.realtime.app-secret}") String appSecret) {
        return client(url, appSecret);
    }

    @Bean
    public WebClient turnWebClient(@Value("${app.calls.realtime.url}") String url,
                                   @Value("${app.calls.turn.api-token}") String apiToken) {
        return client(url, apiToken);
    }

    private static WebClient client(String url, String secret) {
        HttpClient httpClient = HttpClient.create()
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) TIMEOUT.toMillis())
                .responseTimeout(TIMEOUT);
        return WebClient.builder()
                .baseUrl(url)
                .defaultHeader(HttpHeaders.AUTHORIZATION, BEARER + secret)
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .build();
    }
}
