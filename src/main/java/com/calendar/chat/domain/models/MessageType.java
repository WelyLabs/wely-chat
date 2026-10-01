package com.calendar.chat.domain.models;

/**
 * What a message carries.
 *
 * <p>Only {@code TEXT} is produced today. The rest describe the shapes the bucket format was
 * designed to hold — an attachment is a field on the entity already — and are here so that
 * adding one does not change the stored schema.
 */
public enum MessageType {
    TEXT,
    IMAGE,
    VIDEO,
    AUDIO,
    FILE,
    /** System notices, such as "Alice joined the group". */
    SYSTEM
}
