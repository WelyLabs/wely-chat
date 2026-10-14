package com.calendar.chat.domain.models;

/**
 * A track a browser publishes.
 *
 * @param mid       the transceiver it is sent on, from the browser's offer
 * @param trackName how others will ask for it, such as "audio" or "camera"
 */
public record LocalTrack(String mid, String trackName) {}
