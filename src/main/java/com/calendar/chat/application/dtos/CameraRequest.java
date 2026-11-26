package com.calendar.chat.application.dtos;

/** Body of {@code PUT /calls/{id}/camera}: whether the caller's camera is now on. */
public record CameraRequest(boolean on) {}
