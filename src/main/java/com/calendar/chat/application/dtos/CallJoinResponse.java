package com.calendar.chat.application.dtos;

import java.util.List;

/** What the browser needs to open its connection, once in the call. */
public record CallJoinResponse(CallDTO call, List<IceServerDTO> iceServers) {

    /** The browser's RTCIceServer. */
    public record IceServerDTO(List<String> urls, String username, String credential) {}
}
