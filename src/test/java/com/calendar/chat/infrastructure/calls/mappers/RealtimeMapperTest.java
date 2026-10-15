package com.calendar.chat.infrastructure.calls.mappers;

import com.calendar.chat.domain.models.IceServer;
import com.calendar.chat.domain.models.LocalTrack;
import com.calendar.chat.domain.models.RemoteTrack;
import com.calendar.chat.domain.models.SessionDescription;
import com.calendar.chat.domain.models.VideoLayer;
import com.calendar.chat.infrastructure.calls.models.RealtimeSessionDescription;
import com.calendar.chat.infrastructure.calls.models.RealtimeSimulcast;
import com.calendar.chat.infrastructure.calls.models.RealtimeTrack;
import com.calendar.chat.infrastructure.calls.models.RealtimeTrackResult;
import com.calendar.chat.infrastructure.calls.models.TurnIceServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mapstruct.factory.Mappers;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RealtimeMapperTest {

    private final RealtimeMapper mapper = Mappers.getMapper(RealtimeMapper.class);

    @Test
    void sessionDescriptions_shouldRoundTrip() {
        SessionDescription offer = new SessionDescription("offer", "v=0");

        assertThat(mapper.toRealtime(offer)).isEqualTo(new RealtimeSessionDescription("offer", "v=0"));
        assertThat(mapper.toDomain(mapper.toRealtime(offer))).isEqualTo(offer);
    }

    @Test
    void toLocal_shouldPublishByMid() {
        assertThat(mapper.toLocal(new LocalTrack("0", "audio")))
                .isEqualTo(new RealtimeTrack("local", "0", "audio", null, null));
    }

    @Test
    void toRemote_shouldAskForThePreferredLayer_withAFallback() {
        assertThat(mapper.toRemote(new RemoteTrack("s-alice", "camera", VideoLayer.HALF, null)))
                .isEqualTo(new RealtimeTrack("remote", null, "camera", "s-alice",
                        new RealtimeSimulcast("h", "asciibetical", "asciibetical")));
    }

    @Test
    void toRemote_shouldLeaveSimulcastOut_forAudio() {
        assertThat(mapper.toRemote(new RemoteTrack("s-alice", "audio", null, "2")).simulcast()).isNull();
    }

    @ParameterizedTest
    @CsvSource({"FULL,f", "HALF,h", "QUARTER,q"})
    void rid_shouldMatchWhatTheWebAppPublishes(VideoLayer layer, String rid) {
        assertThat(RealtimeMapper.rid(layer)).isEqualTo(rid);
    }

    @Test
    void toRemoteTrack_shouldKeepTheReceivingMid() {
        assertThat(mapper.toRemoteTrack(new RealtimeTrackResult("3", "camera", "s-alice", null, null, null)))
                .isEqualTo(new RemoteTrack("s-alice", "camera", null, "3"));
    }

    @Test
    void toClosing_shouldNameOnlyTheMid() {
        assertThat(mapper.toClosing("3")).isEqualTo(new RealtimeTrack("local", "3", null, null, null));
    }

    @Test
    void toIceServer_shouldKeepTheCredentials() {
        assertThat(mapper.toIceServer(new TurnIceServer(List.of("turn:t:3478"), "user", "pass")))
                .isEqualTo(new IceServer(List.of("turn:t:3478"), "user", "pass"));
    }

    @Test
    void simulcast_shouldBeNull_withoutALayer() {
        assertThat(mapper.simulcast(null)).isNull();
    }
}
