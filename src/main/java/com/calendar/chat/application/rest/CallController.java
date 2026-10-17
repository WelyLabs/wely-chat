package com.calendar.chat.application.rest;

import com.calendar.chat.application.dtos.CallDTO;
import com.calendar.chat.application.dtos.CallJoinResponse;
import com.calendar.chat.application.dtos.LayersRequest;
import com.calendar.chat.application.dtos.PublishRequest;
import com.calendar.chat.application.dtos.SessionDescriptionDTO;
import com.calendar.chat.application.dtos.StartCallRequest;
import com.calendar.chat.application.dtos.SubscribeRequest;
import com.calendar.chat.application.dtos.SubscriptionResponse;
import com.calendar.chat.application.dtos.UnsubscribeRequest;
import com.calendar.chat.application.mappers.CallRestMapper;
import com.calendar.chat.domain.models.CallMedia;
import com.calendar.chat.domain.services.CallService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Supplier;

/**
 * Audio and video calls. The browser negotiates its WebRTC connection through here: this service
 * holds the relay's secret and checks, for each step, that the caller may take it. Who is asking
 * comes from the token; ids in the body are only ever looked up in the caller's own call.
 */
@RestController
@RequestMapping("/calls")
public class CallController {

    private static final String BUSINESS_ID = "businessId";
    private static final String USERNAME = "preferred_username";

    private final CallService callService;
    private final CallRestMapper mapper;

    public CallController(CallService callService, CallRestMapper mapper) {
        this.callService = callService;
        this.mapper = mapper;
    }

    /** Starts a call and rings the other members, or returns the conversation's live call. */
    @PostMapping
    public Mono<CallDTO> start(@AuthenticationPrincipal Jwt jwt, @RequestBody StartCallRequest request) {
        CallMedia media = parsed(() -> CallMedia.valueOf(request.media()));
        return callService.start(caller(jwt), jwt.getClaimAsString(USERNAME), required(request.conversationId()), media)
                .map(mapper::toDTO);
    }

    /** The conversation's live call, or 204 when there is none. */
    @GetMapping("/live")
    public Mono<ResponseEntity<CallDTO>> live(@AuthenticationPrincipal Jwt jwt, @RequestParam String conversationId) {
        return callService.liveCall(caller(jwt), conversationId)
                .map(call -> ResponseEntity.ok(mapper.toDTO(call)))
                .defaultIfEmpty(ResponseEntity.noContent().build());
    }

    /** Enters the call; answers with the ICE servers to connect with. 409 CHT-BUS-009 when full. */
    @PostMapping("/{callId}/join")
    public Mono<CallJoinResponse> join(@AuthenticationPrincipal Jwt jwt, @PathVariable String callId) {
        return callService.join(caller(jwt), callId).map(mapper::toResponse);
    }

    /** Publishes the browser's tracks; answers with the relay's SDP answer. */
    @PostMapping("/{callId}/publish")
    public Mono<SessionDescriptionDTO> publish(@AuthenticationPrincipal Jwt jwt, @PathVariable String callId,
                                               @RequestBody PublishRequest request) {
        return callService.publish(caller(jwt), callId, mapper.toDomain(required(request.offer())),
                        parsed(() -> nonNull(request.tracks()).stream().map(mapper::toDomain).toList()))
                .map(mapper::toDTO);
    }

    /** Receives other participants' tracks. 403 CHT-BUS-010 for a track not published in this call. */
    @PostMapping("/{callId}/subscribe")
    public Mono<SubscriptionResponse> subscribe(@AuthenticationPrincipal Jwt jwt, @PathVariable String callId,
                                                @RequestBody SubscribeRequest request) {
        return callService.subscribe(caller(jwt), callId,
                        parsed(() -> nonNull(request.tracks()).stream().map(mapper::toDomain).toList()))
                .map(mapper::toResponse);
    }

    @PutMapping("/{callId}/renegotiate")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Mono<Void> renegotiate(@AuthenticationPrincipal Jwt jwt, @PathVariable String callId,
                                  @RequestBody SessionDescriptionDTO answer) {
        return callService.renegotiate(caller(jwt), callId, mapper.toDomain(answer));
    }

    /** Switches received cameras to other simulcast layers. */
    @PutMapping("/{callId}/layers")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Mono<Void> layers(@AuthenticationPrincipal Jwt jwt, @PathVariable String callId,
                             @RequestBody LayersRequest request) {
        return callService.chooseLayers(caller(jwt), callId,
                parsed(() -> nonNull(request.choices()).stream().map(mapper::toDomain).toList()));
    }

    @PostMapping("/{callId}/unsubscribe")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Mono<Void> unsubscribe(@AuthenticationPrincipal Jwt jwt, @PathVariable String callId,
                                  @RequestBody UnsubscribeRequest request) {
        return callService.unsubscribe(caller(jwt), callId, nonNull(request.mids()));
    }

    /** Every {@link CallService#HEARTBEAT} while in the call; a silent participant is dropped. */
    @PostMapping("/{callId}/heartbeat")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Mono<Void> heartbeat(@AuthenticationPrincipal Jwt jwt, @PathVariable String callId) {
        return callService.heartbeat(caller(jwt), callId);
    }

    @PostMapping("/{callId}/leave")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Mono<Void> leave(@AuthenticationPrincipal Jwt jwt, @PathVariable String callId) {
        return callService.leave(caller(jwt), callId);
    }

    @PostMapping("/{callId}/decline")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Mono<Void> decline(@AuthenticationPrincipal Jwt jwt, @PathVariable String callId) {
        return callService.decline(caller(jwt), callId);
    }

    private static String caller(Jwt jwt) {
        return jwt.getClaimAsString(BUSINESS_ID);
    }

    private static <T> T required(T value) {
        if (value == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
        }
        return value;
    }

    private static <T> List<T> nonNull(List<T> values) {
        return values == null ? List.of() : values;
    }

    /** An unknown media or layer name is the client's mistake: 400, not 500. */
    private static <T> T parsed(Supplier<T> parsing) {
        try {
            return parsing.get();
        } catch (IllegalArgumentException | NullPointerException unknown) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
        }
    }
}
