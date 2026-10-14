package com.calendar.chat.domain.ports;

import com.calendar.chat.domain.models.Call;

/** The line an ended call leaves in its conversation, in the users' language. */
public interface CallNoticeFormatter {

    String format(Call call);
}
