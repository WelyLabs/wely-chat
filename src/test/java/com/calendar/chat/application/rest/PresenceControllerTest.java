package com.calendar.chat.application.rest;

import com.calendar.chat.domain.models.Presence;
import com.calendar.chat.domain.services.PresenceService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.oauth2.jwt.Jwt;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;

import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PresenceControllerTest {

    @Mock
    private PresenceService presenceService;
    @Mock
    private Jwt jwt;
    @InjectMocks
    private PresenceController controller;

    @Test
    void getPresence_shouldAskOnBehalfOfTheCaller() {
        Presence presence = new Presence("bob", false, Instant.parse("2026-10-11T17:42:00Z"));
        when(jwt.getClaimAsString("businessId")).thenReturn("alice");
        when(presenceService.getPresence("alice", "bob")).thenReturn(Mono.just(presence));

        StepVerifier.create(controller.getPresence(jwt, "bob"))
                .expectNext(presence)
                .verifyComplete();
    }
}
