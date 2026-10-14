package com.calendar.chat.domain.models;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

class CallStatusTest {

    @ParameterizedTest
    @CsvSource({"RINGING,true", "ACTIVE,true", "ENDED,false", "MISSED,false", "DECLINED,false"})
    void isLive_shouldBeTrueOnlyWhileTheCallCanBeJoined(CallStatus status, boolean live) {
        assertThat(status.isLive()).isEqualTo(live);
    }
}
