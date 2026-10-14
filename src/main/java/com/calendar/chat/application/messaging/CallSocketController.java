package com.calendar.chat.application.messaging;

import com.calendar.chat.application.dtos.CallEventDTO;
import com.calendar.chat.application.mappers.CallRestMapper;
import com.calendar.chat.domain.services.CallService;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Flux;

/** Call events over RSocket, beside the chat stream: someone calls, joins, leaves, publishes. */
@Controller
public class CallSocketController {

    private final CallService callService;
    private final CallRestMapper mapper;

    public CallSocketController(CallService callService, CallRestMapper mapper) {
        this.callService = callService;
        this.mapper = mapper;
    }

    /** The events addressed to the caller, for as long as the app is open. */
    @MessageMapping("call.events")
    public Flux<CallEventDTO> streamEvents(@AuthenticationPrincipal Jwt jwt) {
        return callService.streamEvents(jwt.getClaimAsString("businessId")).map(mapper::toDTO);
    }
}
