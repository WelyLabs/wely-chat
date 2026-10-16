package com.calendar.chat.domain.models;

import java.util.List;

/** One page of wely-users' tombstones; {@code next} is the opaque cursor of the following page, null on the last. */
public record DeletedUsersPage(List<DeletedUser> items, String next) {

    public DeletedUsersPage {
        items = items == null ? List.of() : List.copyOf(items);
    }
}
