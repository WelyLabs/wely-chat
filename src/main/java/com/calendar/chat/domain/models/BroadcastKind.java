package com.calendar.chat.domain.models;

/** Why a message is pushed live: a client appends a NEW one and replaces an UPDATED one by id. */
public enum BroadcastKind {
    NEW,
    /** Already delivered, changed since: a photo removed, for now. */
    UPDATED
}
