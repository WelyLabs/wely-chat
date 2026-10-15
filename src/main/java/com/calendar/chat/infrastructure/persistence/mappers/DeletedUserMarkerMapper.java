package com.calendar.chat.infrastructure.persistence.mappers;

import com.calendar.chat.domain.models.DeletedUserMarker;
import com.calendar.chat.infrastructure.persistence.models.entities.DeletedUserMarkerEntity;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/** The marker's id is the businessId. */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface DeletedUserMarkerMapper {

    @Mapping(target = "userId", source = "id")
    DeletedUserMarker toDomain(DeletedUserMarkerEntity entity);

    @Mapping(target = "id", source = "userId")
    DeletedUserMarkerEntity toEntity(DeletedUserMarker marker);
}
