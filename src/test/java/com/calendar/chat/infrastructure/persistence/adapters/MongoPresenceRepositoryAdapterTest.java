package com.calendar.chat.infrastructure.persistence.adapters;

import com.calendar.chat.domain.models.Presence;
import com.calendar.chat.infrastructure.persistence.models.entities.LastSeenEntity;
import com.calendar.chat.infrastructure.persistence.models.entities.PresenceConnectionEntity;
import com.calendar.chat.infrastructure.persistence.repositories.LastSeenRepository;
import com.calendar.chat.infrastructure.persistence.repositories.PresenceConnectionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MongoPresenceRepositoryAdapterTest {

    private static final Instant NOW = Instant.parse("2026-10-11T18:00:00Z");

    @Mock
    private PresenceConnectionRepository connectionRepository;
    @Mock
    private LastSeenRepository lastSeenRepository;

    private MongoPresenceRepositoryAdapter adapter;

    @BeforeEach
    void setUp() {
        adapter = new MongoPresenceRepositoryAdapter(connectionRepository, lastSeenRepository);
    }

    @Test
    void connect_shouldSaveTheConnectionAndTheLastSeenTime() {
        when(connectionRepository.save(any())).thenAnswer(call -> Mono.just(call.getArgument(0)));
        when(lastSeenRepository.save(any())).thenAnswer(call -> Mono.just(call.getArgument(0)));

        StepVerifier.create(adapter.connect("c-1", "alice", "pod-1", NOW)).verifyComplete();

        ArgumentCaptor<PresenceConnectionEntity> saved = ArgumentCaptor.forClass(PresenceConnectionEntity.class);
        verify(connectionRepository).save(saved.capture());
        assertThat(saved.getValue()).usingRecursiveComparison()
                .isEqualTo(new PresenceConnectionEntity("c-1", "alice", "pod-1", NOW));
    }

    @Test
    void disconnect_shouldDeleteTheConnectionAndRecordTheLastSeenTime() {
        when(connectionRepository.deleteById("c-1")).thenReturn(Mono.empty());
        when(lastSeenRepository.save(any())).thenAnswer(call -> Mono.just(call.getArgument(0)));

        StepVerifier.create(adapter.disconnect("c-1", "alice", NOW)).verifyComplete();

        ArgumentCaptor<LastSeenEntity> seen = ArgumentCaptor.forClass(LastSeenEntity.class);
        verify(lastSeenRepository).save(seen.capture());
        assertThat(seen.getValue()).usingRecursiveComparison().isEqualTo(new LastSeenEntity("alice", NOW));
    }

    @Test
    void refresh_shouldTouchEveryConnection_andEachUserOnce() {
        when(connectionRepository.refresh(any(), any())).thenReturn(Mono.just(3L));
        when(lastSeenRepository.saveAll(anyList())).thenAnswer(call -> Flux.fromIterable(call.<List<LastSeenEntity>>getArgument(0)));

        StepVerifier.create(adapter.refresh(Map.of("c-1", "alice", "c-2", "alice", "c-3", "bob"), NOW)).verifyComplete();

        verify(connectionRepository).refresh(Set.of("c-1", "c-2", "c-3"), NOW);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<LastSeenEntity>> seen = ArgumentCaptor.forClass(List.class);
        verify(lastSeenRepository).saveAll(seen.capture());
        assertThat(seen.getValue()).extracting(LastSeenEntity::getId).containsExactlyInAnyOrder("alice", "bob");
    }

    @Test
    void find_shouldCombineALiveConnectionWithTheLastSeenTime() {
        when(connectionRepository.existsByUserIdAndSeenAtAfter("alice", NOW)).thenReturn(Mono.just(true));
        when(lastSeenRepository.findById("alice")).thenReturn(Mono.just(new LastSeenEntity("alice", NOW)));

        StepVerifier.create(adapter.find("alice", NOW))
                .expectNext(new Presence("alice", true, NOW))
                .verifyComplete();
    }

    @Test
    void find_shouldAnswerOffline_forAUserNeverSeen() {
        when(connectionRepository.existsByUserIdAndSeenAtAfter("bob", NOW)).thenReturn(Mono.just(false));
        when(lastSeenRepository.findById("bob")).thenReturn(Mono.empty());

        StepVerifier.create(adapter.find("bob", NOW))
                .expectNext(new Presence("bob", false, null))
                .verifyComplete();
    }
}
