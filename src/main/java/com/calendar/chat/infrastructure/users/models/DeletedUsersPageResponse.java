package com.calendar.chat.infrastructure.users.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

/** One page of wely-users' GET /user-service/internal/deleted-users; next is null on the last page. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record DeletedUsersPageResponse(List<DeletedUserResponse> items, String next) {}
