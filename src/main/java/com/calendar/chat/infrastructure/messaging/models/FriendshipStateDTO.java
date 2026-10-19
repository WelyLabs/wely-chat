package com.calendar.chat.infrastructure.messaging.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * A value of wely-social's compacted {@code FRIENDSHIP_STATE}, keyed {@code a:b}: the pair while they are
 * friends. The end of a friendship is a tombstone (no value), never this record.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record FriendshipStateDTO(String a, String b, Boolean friends, Long version) {
}
