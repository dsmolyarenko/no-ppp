package org.no.ppp.sos.model;

import java.io.IOException;
import java.io.UncheckedIOException;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Packet {

    private String id;

    private byte[] data;

    boolean open;

    boolean close;

    public Packet() {
    }

    public Packet(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public Packet setId(String id) {
        this.id = id;
        return this;
    }

    public byte[] getData() {
        return data;
    }

    public Packet setData(byte[] data) {
        this.data = data;
        return this;
    }

    public boolean isOpen() {
        return open;
    }

    public Packet setOpen(boolean open) {
        this.open = open;
        return this;
    }

    public boolean isClose() {
        return close;
    }

    public Packet setClose(boolean close) {
        this.close = close;
        return this;
    }

    public static Packet of(byte[] bytes) throws IOException {
        return om.readValue(bytes, Packet.class);
    }

    public String asString() {
        try {
            return om.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + ":" + asString();
    }

    public static Packet of(String string) {
        try {
            return om.readValue(string, Packet.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static ObjectMapper om = new ObjectMapper().setSerializationInclusion(Include.NON_DEFAULT);
}
