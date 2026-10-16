package com.calendar.chat.infrastructure.users.mappers;

import com.calendar.chat.domain.models.DeletedUser;
import com.calendar.chat.domain.models.DeletedUsersPage;
import com.calendar.chat.infrastructure.users.models.DeletedUserResponse;
import com.calendar.chat.infrastructure.users.models.DeletedUsersPageResponse;
import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;

/** Translates wely-users' listing of deleted accounts into the domain; a UUID becomes its canonical lowercase form. */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface DeletedUsersMapper {

    DeletedUsersPage toPage(DeletedUsersPageResponse response);

    DeletedUser toDeletedUser(DeletedUserResponse item);
}
