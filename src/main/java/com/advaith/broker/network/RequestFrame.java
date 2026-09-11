package com.advaith.broker.network;

/**
 * One complete, size-delimited Kafka request: the raw header+body bytes
 * (the 4-byte length prefix itself is consumed by Connection and not
 * included here — callers never need it again) plus the connection it
 * arrived on, so a handler can eventually write the response back to the
 * right socket.
 */
public final class RequestFrame {

    private final Connection connection;
    private final byte[] payload;

    public RequestFrame(Connection connection, byte[] payload) {
        this.connection = connection;
        this.payload = payload;
    }

    public Connection connection() {
        return connection;
    }

    /** Header + body bytes, i.e. everything after the INT32 size field. */
    public byte[] payload() {
        return payload;
    }
}
