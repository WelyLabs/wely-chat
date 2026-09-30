package com.calendar.chat.configuration;

import com.calendar.chat.domain.ports.ChatRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.security.config.annotation.rsocket.RSocketSecurity;
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder;
import org.springframework.security.rsocket.core.PayloadSocketAcceptorInterceptor;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers {@link RSocketSecurityConfig#rsocketInterceptor(RSocketSecurity)}.
 *
 * <p>The pairing inside it reads oddly at first — {@code anyRequest().authenticated()}
 * followed by {@code anyExchange().permitAll()} — and looks like one rule cancelling the
 * other. It does not. The two matchers cover different things: {@code anyRequest} covers
 * request payloads, {@code anyExchange} covers every exchange including the SETUP frame.
 * The SETUP frame is what <em>carries</em> the credentials, so requiring authentication
 * on it would refuse the connection before any token could be presented. This is the
 * arrangement Spring Security's own RSocket documentation prescribes.
 */
@SpringBootTest
@ActiveProfiles("test")
class RSocketSecurityConfigTest {

    @Autowired
    private ApplicationContext context;

    @MockitoBean
    private ReactiveJwtDecoder jwtDecoder;

    @MockitoBean
    private ChatRepository chatRepository;

    @Test
    void rsocketInterceptor_shouldBeRegistered() {
        assertThat(context.getBean(PayloadSocketAcceptorInterceptor.class)).isNotNull();
    }

    @Test
    @DisplayName("a single interceptor: a second would silently override this one")
    void rsocketInterceptor_shouldBeTheOnlyOneDeclared() {
        assertThat(context.getBeansOfType(PayloadSocketAcceptorInterceptor.class)).hasSize(1);
    }

    @Test
    @DisplayName("the interceptor is built from the context's RSocketSecurity")
    void rsocketInterceptor_shouldBuildFromTheContextSecurity() {
        RSocketSecurityConfig config = context.getBean(RSocketSecurityConfig.class);
        RSocketSecurity rsocket = context.getBean(RSocketSecurity.class);

        PayloadSocketAcceptorInterceptor interceptor = config.rsocketInterceptor(rsocket);

        assertThat(interceptor).isNotNull();
    }

    @Test
    void context_shouldExposeRSocketSecurityAsAPrototype() {
        // RSocketSecurity carries builder state: two injections must yield two
        // instances, or one bean's configuration leaks into the other.
        assertThat(context.getBean(RSocketSecurity.class))
                .isNotSameAs(context.getBean(RSocketSecurity.class));
    }
}
