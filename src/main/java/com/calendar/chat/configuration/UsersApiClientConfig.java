package com.calendar.chat.configuration;

import io.netty.channel.ChannelOption;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;

/** Client for wely-users' internal list of deleted accounts, with its own reader secret; blank values set nothing. */
@Configuration
public class UsersApiClientConfig {

    static final Duration TIMEOUT = Duration.ofSeconds(10);
    public static final String SECRET_HEADER = "X-Internal-Secret";

    @Bean
    public WebClient usersWebClient(@Value("${app.users-api.url:}") String usersApiUrl,
                                    @Value("${app.users-api.deleted-users-secret:}") String secret) {
        HttpClient httpClient = HttpClient.create()
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) TIMEOUT.toMillis())
                .responseTimeout(TIMEOUT);
        WebClient.Builder builder = WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(httpClient));
        if (!usersApiUrl.isBlank()) {
            builder.baseUrl(usersApiUrl);
        }
        if (!secret.isBlank()) {
            builder.defaultHeader(SECRET_HEADER, secret);
        }
        return builder.build();
    }
}
