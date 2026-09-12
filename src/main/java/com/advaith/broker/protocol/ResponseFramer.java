package com.advaith.broker.protocol;

import java.nio.ByteBuffer;

/**
 * Wraps a handler's response body with the response header and the
 * 4-byte length prefix every API shares — extracted out of
 * RequestDispatcher (PRD §7.3) so a handler that must answer later
 * instead of immediately (Fetch long-polling) can produce a byte-identical
 * frame from outside the dispatcher's own synchronous return path. There
 * must be exactly one place that knows this wrapping, used by both paths,
 * or the two will eventually drift apart.
 */
public final class ResponseFramer {

    private ResponseFramer() {}

    public static ByteBuffer frame(int correlationId, short responseHeaderVersion, byte[] body) {
        ProtocolWriter full = new ProtocolWriter(body.length + 8);
        new ResponseHeader(correlationId).write(full, responseHeaderVersion);
        full.writeRawBytes(body);
        byte[] bytes = full.toByteArray();

        ByteBuffer framed = ByteBuffer.allocate(4 + bytes.length);
        framed.putInt(bytes.length);
        framed.put(bytes);
        framed.flip(); // switch to read mode: Connection.enqueueResponse only ever reads from this
        return framed;
    }
}
