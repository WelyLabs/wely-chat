package com.calendar.chat.domain.models;

/**
 * What a message carries.
 *
 * <p>{@code TEXT} and {@code SYSTEM} are produced today. The rest describe the shapes the bucket format was
 * designed to hold — an attachment is a field on the entity already — and are here so that
 * adding one does not change the stored schema.
 */
public enum MessageType {
    TEXT,
    IMAGE,
    VIDEO,
    AUDIO,
    FILE,
    /** Notices posted by the application, such as an event's date being fixed. */
    SYSTEM
}
