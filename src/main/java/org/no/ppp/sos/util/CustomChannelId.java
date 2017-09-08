package org.no.ppp.sos.util;

import io.netty.channel.ChannelId;

public final class CustomChannelId implements ChannelId {

    private static final long serialVersionUID = -1L;

    private final String id;

    public CustomChannelId(String id) {
        this.id = id;
    }

    @Override
    public int compareTo(ChannelId o) {
        return asShortText().compareTo(o.asShortText());
    }

    @Override
    public String asShortText() {
        return id;
    }

    @Override
    public String asLongText() {
        return id;
    }
}