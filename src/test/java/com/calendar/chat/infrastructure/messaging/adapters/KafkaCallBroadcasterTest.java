package com.calendar.chat.infrastructure.messaging.adapters;

import com.calendar.chat.domain.models.Call;
import com.calendar.chat.domain.models.CallEvent;
import com.calendar.chat.domain.models.CallEventType;
import com.calendar.chat.domain.models.CallMedia;
import com.calendar.chat.infrastructure.messaging.mappers.CallEventBroadcastMapper;
import com.calendar.chat.infrastructure.messaging.models.CallEventBroadcastDTO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;
import org.mockito.ArgumentCaptor;
import org.springframework.cloud.stream.function.StreamBridge;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaCallBroadcasterTest {

    private final StreamBridge streamBridge = mock(StreamBridge.class);
    private final CallEventBroadcastMapper mapper = Mappers.getMapper(CallEventBroadcastMapper.class);
    private KafkaCallBroadcaster broadcaster;

    @BeforeEach
    void setUp() {
        broadcaster = new KafkaCallBroadcaster(streamBridge, mapper);
    }

    private static CallEvent ringing(String recipientId) {
        return new CallEvent(CallEventType.RINGING, recipientId, "alice",
                Call.ringing("conv-1", "alice", "Alice", CallMedia.VIDEO, Instant.EPOCH, List.of("alice", recipientId)));
    }

    @Test
    void broadcast_shouldSendTheEventToTheCallBinding() {
        when(streamBridge.send(eq(KafkaCallBroadcaster.DESTINATION), any(Object.class))).thenReturn(true);

        StepVerifier.create(broadcaster.broadcast(ringing("bob"))).verifyComplete();

        verify(streamBridge).send(KafkaCallBroadcaster.DESTINATION, mapper.toBroadcastDTO(ringing("bob")));
    }

    @Test
    void broadcast_shouldCarryTheCallerName_forRingingScreens() {
        when(streamBridge.send(eq(KafkaCallBroadcaster.DESTINATION), any(Object.class))).thenReturn(true);

        StepVerifier.create(broadcaster.broadcast(ringing("bob"))).verifyComplete();

        ArgumentCaptor<Object> sent = ArgumentCaptor.forClass(Object.class);
        verify(streamBridge).send(eq(KafkaCallBroadcaster.DESTINATION), sent.capture());
        assertThat(((CallEventBroadcastDTO) sent.getValue()).call().startedByName()).isEqualTo("Alice");
    }

    @Test
    void broadcast_shouldNotFail_whenTheBinderRefuses() {
        when(streamBridge.send(eq(KafkaCallBroadcaster.DESTINATION), any(Object.class))).thenReturn(false);

        StepVerifier.create(broadcaster.broadcast(ringing("bob"))).verifyComplete();
    }

    @Test
    void subscribe_shouldReceiveTheRecordsReadBackForTheUser() {
        StepVerifier.create(broadcaster.subscribe("bob").take(1))
                .then(() -> {
                    broadcaster.accept(mapper.toBroadcastDTO(ringing("carol")));
                    broadcaster.accept(mapper.toBroadcastDTO(ringing("bob")));
                })
                .expectNext(ringing("bob"))
                .verifyComplete();
    }
}
