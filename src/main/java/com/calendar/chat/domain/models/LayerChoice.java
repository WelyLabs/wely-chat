package com.calendar.chat.domain.models;

/**
 * Switches a received camera to another simulcast layer.
 *
 * @param mid    the receiving transceiver
 * @param userId who publishes the camera
 */
public record LayerChoice(String mid, String userId, String trackName, VideoLayer layer) {}
