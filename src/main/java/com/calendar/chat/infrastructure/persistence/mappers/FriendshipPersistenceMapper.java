package com.calendar.chat.infrastructure.persistence.mappers;

import com.calendar.chat.domain.models.Friendship;
import com.calendar.chat.infrastructure.persistence.models.entities.FriendshipEntity;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/** A stored pair is written as friends; ending one is an update, never a mapping. */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface FriendshipPersistenceMapper {

    @Mapping(target = "id", expression = "java(friendship.pairId())")
    @Mapping(target = "a", source = "userA")
    @Mapping(target = "b", source = "userB")
    @Mapping(target = "friends", constant = "true")
    FriendshipEntity toEntity(Friendship friendship);
}
