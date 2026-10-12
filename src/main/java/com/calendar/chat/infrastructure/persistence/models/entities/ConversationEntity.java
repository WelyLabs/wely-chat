package com.calendar.chat.infrastructure.persistence.models.entities;

import com.calendar.chat.infrastructure.persistence.models.dtos.MessageEntity;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceCreator;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;
import java.util.List;

@Document(collection = "conversations")
@Getter
@Setter
@AllArgsConstructor(onConstructor_ = @PersistenceCreator)
public class ConversationEntity {
    @Id
    private String id;
    
    @Indexed
    private List<String> participantIds;
    
    private MessageEntity lastMessage;

    private LocalDateTime updatedAt;

    /** DIRECT or EVENT. Absent on conversations stored before groups existed, which are DIRECT. */
    private String type;

    /** One conversation per event; sparse, since a private conversation has none. */
    @Indexed(unique = true, sparse = true)
    private String eventId;

    private String title;

    /** The version of the member list last applied, from wely-events. */
    private Long groupVersion;

    /** A private conversation between these participants. A factory, so MapStruct sees one constructor. */
    public static ConversationEntity direct(String id, List<String> participantIds, MessageEntity lastMessage,
                                            LocalDateTime updatedAt) {
        return new ConversationEntity(id, participantIds, lastMessage, updatedAt, null, null, null, null);
    }
}
