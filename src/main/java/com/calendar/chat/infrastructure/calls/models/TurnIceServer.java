package com.calendar.chat.infrastructure.calls.models;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.util.List;

/** One server in Cloudflare's answer; {@code urls} may come as one string or a list. */
public record TurnIceServer(@JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY) List<String> urls,
                            String username, String credential) {}
