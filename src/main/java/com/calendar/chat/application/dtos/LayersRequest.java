package com.calendar.chat.application.dtos;

import java.util.List;

/** Body of {@code PUT /calls/{id}/layers}: which simulcast layer each received camera should use. */
public record LayersRequest(List<Choice> choices) {

    public record Choice(String mid, String userId, String trackName, String layer) {}
}
