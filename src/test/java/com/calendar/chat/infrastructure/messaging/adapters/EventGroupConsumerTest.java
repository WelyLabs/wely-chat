package com.calendar.chat.infrastructure.messaging.adapters;

import com.calendar.chat.domain.models.EventGroup;
import com.calendar.chat.domain.services.ChatService;
import com.calendar.chat.infrastructure.messaging.mappers.EventGroupMapper;
import com.calendar.chat.infrastructure.messaging.models.EventGroupChangedDTO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mapstruct.factory.Mappers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EventGroupConsumerTest {

    @Mock
    private ChatService chatService;

    private EventGroupConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new EventGroupConsumer(chatService, Mappers.getMapper(EventGroupMapper.class));
    }

    private static Message<EventGroupChangedDTO> record(String eventId) {
        return MessageBuilder.withPayload(new EventGroupChangedDTO(eventId, "t", "a", List.of("a"), 1)).build();
    }

    @Test
    void eventGroup_shouldApplyEachRecord() {
        when(chatService.applyEventGroup(any())).thenReturn(Mono.empty());

        consumer.eventGroup().accept(Flux.just(record("e1"), record("e2")));

        verify(chatService, timeout(2000)).applyEventGroup(new EventGroup("e1", "t", "a", List.of("a"), 1));
        verify(chatService, timeout(2000)).applyEventGroup(new EventGroup("e2", "t", "a", List.of("a"), 1));
    }

    @Test
    @DisplayName("a failing record is retried, then dropped without stopping the stream")
    void eventGroup_shouldRetryThenKeepConsuming() {
        AtomicInteger attempts = new AtomicInteger();
        EventGroup failing = new EventGroup("e1", "t", "a", List.of("a"), 1);
        EventGroup next = new EventGroup("e2", "t", "a", List.of("a"), 1);
        when(chatService.applyEventGroup(failing)).thenReturn(Mono.defer(() -> {
            attempts.incrementAndGet();
            return Mono.error(new IllegalStateException("mongo down"));
        }));
        when(chatService.applyEventGroup(next)).thenReturn(Mono.empty());

        consumer.eventGroup().accept(Flux.just(record("e1"), record("e2")));

        // One attempt plus three retries with backoff from 1s, then the next record.
        verify(chatService, timeout(20_000)).applyEventGroup(next);
        assertThat(attempts.get()).isEqualTo(4);
    }
}
