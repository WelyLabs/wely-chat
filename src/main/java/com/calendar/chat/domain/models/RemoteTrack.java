package com.calendar.chat.domain.models;

/**
 * A published track as the media relay knows it: by the publisher's session.
 *
 * @param mid   the receiving transceiver; null until the relay has assigned it
 * @param layer the simulcast layer to forward; null for audio
 */
public record RemoteTrack(String sessionId, String trackName, VideoLayer layer, String mid) {}
