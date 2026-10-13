package com.calendar.chat.infrastructure.persistence.models.dtos;

import org.springframework.data.mongodb.core.mapping.Field;

/** A message's photo inside a bucket: its storage key and size. A removed photo has no key. */
public record PhotoEntity(
        @Field("k") String key,
        @Field("w") int width,
        @Field("h") int height,
        @Field("rm") boolean removed
) {}
