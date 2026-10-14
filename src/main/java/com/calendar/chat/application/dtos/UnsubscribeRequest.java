package com.calendar.chat.application.dtos;

import java.util.List;

/** Body of {@code POST /calls/{id}/unsubscribe}: the receiving transceivers to close. */
public record UnsubscribeRequest(List<String> mids) {}
