package com.calendar.chat.configuration;

import com.calendar.chat.domain.ports.CallBroadcaster;
import com.calendar.chat.domain.ports.CallNoticeFormatter;
import com.calendar.chat.domain.ports.CallRepository;
import com.calendar.chat.domain.ports.DeletedUserMarkers;
import com.calendar.chat.domain.ports.DeletedUsersSource;
import com.calendar.chat.domain.ports.FriendshipDirectory;
import com.calendar.chat.domain.ports.IceServerProvider;
import com.calendar.chat.domain.ports.MediaRelay;
import com.calendar.chat.domain.services.AccountErasureService;
import com.calendar.chat.domain.services.AccountGuard;
import com.calendar.chat.domain.services.CallService;
import com.calendar.chat.domain.ports.ChatRepository;
import com.calendar.chat.domain.ports.MessageBroadcaster;
import com.calendar.chat.domain.ports.PresenceRepository;
import com.calendar.chat.domain.services.ChatService;
import com.calendar.chat.domain.services.AccountReconciliationService;
import com.calendar.chat.domain.services.EventGroupService;
import com.calendar.chat.domain.services.FriendshipService;
import com.calendar.chat.domain.services.LiveSessions;
import com.calendar.chat.domain.services.PhotoService;
import com.calendar.chat.domain.ports.ConversationPhotoRepository;
import com.calendar.chat.domain.ports.MediaStore;
import com.calendar.chat.domain.services.PresenceService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.repository.config.EnableReactiveMongoRepositories;

import java.time.Clock;
import java.time.Duration;

/**
 * Wires the chat domain service by hand so the domain layer carries no Spring
 * annotation and stays testable without a context.
 */
@Configuration
@EnableReactiveMongoRepositories(basePackages = "com.calendar.chat.infrastructure.persistence.repositories")
public class ChatApplicationConfig {

    @Bean
    public AccountGuard accountGuard(DeletedUserMarkers markers) {
        return new AccountGuard(markers);
    }

    @Bean
    public FriendshipService friendshipService(FriendshipDirectory directory, AccountGuard accountGuard) {
        return new FriendshipService(directory, accountGuard);
    }

    @Bean
    public LiveSessions liveSessions(AccountGuard accountGuard) {
        return new LiveSessions(accountGuard);
    }

    @Bean
    public ChatService chatService(ChatRepository chatRepository,
                                  MessageBroadcaster messageBroadcaster,
                                  AccountGuard accountGuard,
                                  FriendshipService friendshipService) {
        return new ChatService(chatRepository, messageBroadcaster, accountGuard, friendshipService, Clock.systemUTC());
    }

    @Bean
    public PhotoService photoService(ChatRepository chatRepository, ConversationPhotoRepository photoRepository,
                                     MediaStore mediaStore, ChatService chatService, AccountGuard accountGuard) {
        return new PhotoService(chatRepository, photoRepository, mediaStore, chatService, accountGuard,
                Clock.systemUTC());
    }

    @Bean
    public EventGroupService eventGroupService(ChatRepository chatRepository, PhotoService photoService) {
        return new EventGroupService(chatRepository, photoService);
    }

    /** The instance id is the broadcast one: each pod's own, which its connections carry. */
    @Bean
    public PresenceService presenceService(PresenceRepository presenceRepository, ChatRepository chatRepository,
                                          AccountGuard accountGuard,
                                          @Value("${chat.broadcast.instance-id}") String instanceId) {
        return new PresenceService(presenceRepository, chatRepository, accountGuard, instanceId, Clock.systemUTC());
    }

    @Bean
    public CallService callService(CallRepository callRepository, ChatRepository chatRepository, MediaRelay mediaRelay,
                                   IceServerProvider iceServerProvider, CallBroadcaster callBroadcaster,
                                   ChatService chatService, CallNoticeFormatter noticeFormatter,
                                   AccountGuard accountGuard,
                                   @Value("${app.calls.max-participants}") int maxParticipants) {
        return new CallService(callRepository, chatRepository, mediaRelay, iceServerProvider, callBroadcaster,
                chatService, noticeFormatter, accountGuard, maxParticipants, Clock.systemUTC());
    }

    @Bean
    public AccountErasureService accountErasureService(DeletedUserMarkers markers, ChatRepository chatRepository,
                                                       PresenceRepository presenceRepository, CallService callService,
                                                       LiveSessions liveSessions,
                                                       FriendshipService friendshipService) {
        return new AccountErasureService(markers, chatRepository, presenceRepository, callService, liveSessions,
                friendshipService, Clock.systemUTC());
    }

    @Bean
    public AccountReconciliationService accountReconciliationService(
            DeletedUsersSource source, DeletedUserMarkers markers, AccountErasureService accountErasureService,
            @Value("${app.account-erasure.reconciliation.recent-window:48h}") Duration recentWindow,
            @Value("${app.account-erasure.reconciliation.page-size:500}") int pageSize) {
        return new AccountReconciliationService(source, markers, accountErasureService, Clock.systemUTC(),
                recentWindow, pageSize);
    }
}
