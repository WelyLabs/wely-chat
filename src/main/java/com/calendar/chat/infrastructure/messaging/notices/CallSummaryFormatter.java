package com.calendar.chat.infrastructure.messaging.notices;

import com.calendar.chat.domain.models.Call;
import com.calendar.chat.domain.models.CallMedia;
import com.calendar.chat.domain.ports.CallNoticeFormatter;
import org.springframework.stereotype.Component;

import java.time.Duration;

/** The line a call leaves in its conversation, in French like the rest of the interface. */
@Component
public class CallSummaryFormatter implements CallNoticeFormatter {

    @Override
    public String format(Call call) {
        String kind = call.media() == CallMedia.VIDEO ? "Appel vidéo" : "Appel audio";
        return switch (call.status()) {
            case MISSED -> "📞 " + kind + " manqué";
            case DECLINED -> "📞 " + kind + " refusé";
            default -> "📞 " + kind + " · " + duration(Duration.between(call.startedAt(), call.endedAt()));
        };
    }

    /** "45 s", "12 min", "1 h 05". */
    static String duration(Duration length) {
        if (length.toMinutes() < 1) {
            return length.toSeconds() + " s";
        }
        if (length.toHours() < 1) {
            return length.toMinutes() + " min";
        }
        return length.toHours() + " h " + String.format("%02d", length.toMinutesPart());
    }
}
