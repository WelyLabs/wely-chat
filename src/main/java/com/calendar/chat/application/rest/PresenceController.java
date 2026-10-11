package com.calendar.chat.application.rest;

import com.calendar.chat.domain.models.Presence;
import com.calendar.chat.domain.services.PresenceService;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/presence")
public class PresenceController {

    private final PresenceService presenceService;

    public PresenceController(PresenceService presenceService) {
        this.presenceService = presenceService;
    }

    /** Whether this user is online, for someone who shares a conversation with them. */
    @GetMapping("{userId}")
    public Mono<Presence> getPresence(@AuthenticationPrincipal Jwt jwt, @PathVariable String userId) {
        return presenceService.getPresence(jwt.getClaimAsString("businessId"), userId);
    }
}
