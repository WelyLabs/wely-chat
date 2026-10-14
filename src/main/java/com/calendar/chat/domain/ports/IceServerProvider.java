package com.calendar.chat.domain.ports;

import com.calendar.chat.domain.models.IceServer;
import reactor.core.publisher.Mono;

import java.util.List;

/** STUN and TURN servers for a browser joining a call, with short-lived TURN credentials. */
public interface IceServerProvider {

    Mono<List<IceServer>> iceServers();
}
