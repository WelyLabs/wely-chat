package com.calendar.chat.application.messaging;

import com.calendar.chat.application.dtos.CallEventDTO;
import com.calendar.chat.application.mappers.CallRestMapper;
import com.calendar.chat.domain.services.CallService;
import com.calendar.chat.domain.services.LiveSessions;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Flux;

/** Call events over RSocket, beside the chat stream: someone calls, joins, leaves, publishes. */
@Controller
public class CallSocketController {

    private final CallService callService;
    private final LiveSessions liveSessions;
    private final CallRestMapper mapper;

    public CallSocketController(CallService callService, LiveSessions liveSessions, CallRestMapper mapper) {
        this.callService = callService;
        this.liveSessions = liveSessions;
        this.mapper = mapper;
    }

    /** The events addressed to the caller, for as long as the app is open and the account exists. */
    @MessageMapping("call.events")
    public Flux<CallEventDTO> streamEvents(@AuthenticationPrincipal Jwt jwt) {
        String userId = jwt.getClaimAsString("businessId");
        return liveSessions.attach(userId, callService.streamEvents(userId).map(mapper::toDTO));
    }
}
