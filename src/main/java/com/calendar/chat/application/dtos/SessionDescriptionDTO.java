package com.calendar.chat.application.dtos;

/** An offer or an answer, as {@code RTCSessionDescription} gives and takes it. */
public record SessionDescriptionDTO(String type, String sdp) {}
