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
import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;

/** Between the domain and Cloudflare's JSON. Simulcast layers become the rids the browser publishes. */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface RealtimeMapper {

    String LOCAL = "local";
    String REMOTE = "remote";
    /** Falls back on the next layer, in rid order, when the preferred one is not sent. */
    String ASCIIBETICAL = "asciibetical";

    RealtimeSessionDescription toRealtime(SessionDescription description);

    SessionDescription toDomain(RealtimeSessionDescription description);

    IceServer toIceServer(TurnIceServer server);

    default RealtimeTrack toLocal(LocalTrack track) {
        return new RealtimeTrack(LOCAL, track.mid(), track.trackName(), null, null);
    }

    default RealtimeTrack toRemote(RemoteTrack track) {
        return new RealtimeTrack(REMOTE, track.mid(), track.trackName(), track.sessionId(), simulcast(track.layer()));
    }

    default RemoteTrack toRemoteTrack(RealtimeTrackResult result) {
        return new RemoteTrack(result.sessionId(), result.trackName(), null, result.mid());
    }

    /** Only the mid identifies the transceiver to close. */
    default RealtimeTrack toClosing(String mid) {
        return new RealtimeTrack(LOCAL, mid, null, null, null);
    }

    default RealtimeSimulcast simulcast(VideoLayer layer) {
        return layer == null ? null : new RealtimeSimulcast(rid(layer), ASCIIBETICAL, ASCIIBETICAL);
    }

    /** The rids the web app publishes its camera with: full, half and quarter resolution. */
    static String rid(VideoLayer layer) {
        return switch (layer) {
            case FULL -> "f";
            case HALF -> "h";
            case QUARTER -> "q";
        };
    }
}
