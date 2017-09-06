package org.no.ppp.sos.util;

import io.netty.buffer.ByteBuf;

public class Utils {

    public static byte[] getBytes(ByteBuf buffer) {
        byte[] bytes = new byte[buffer.readableBytes()];
        buffer.readBytes(bytes);
        return bytes;
    }

}
