package com.calendar.chat.domain.models;

import java.util.List;

/**
 * What a browser needs to connect once in a call.
 *
 * @param sessionId  its session on the media relay
 * @param iceServers STUN and TURN servers to reach the relay with
 */
public record CallJoin(Call call, String sessionId, List<IceServer> iceServers) {}
