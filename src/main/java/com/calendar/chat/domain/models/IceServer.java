package com.calendar.chat.domain.models;

import java.util.List;

/**
 * A STUN or TURN server, in the shape of the browser's RTCIceServer.
 *
 * @param username   null for STUN
 * @param credential null for STUN; short-lived for TURN
 */
public record IceServer(List<String> urls, String username, String credential) {

    public IceServer {
        urls = List.copyOf(urls);
    }
}
