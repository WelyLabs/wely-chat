package com.calendar.chat.domain.models;

/**
 * A track a participant asks to receive, named by who publishes it.
 *
 * @param layer the simulcast layer wanted for a camera; null for audio
 */
public record TrackRequest(String userId, String trackName, VideoLayer layer) {}
