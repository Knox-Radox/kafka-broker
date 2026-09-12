package com.advaith.broker.api;

import com.advaith.broker.network.Connection;
import com.advaith.broker.protocol.ResponseFramer;

/**
 * Everything a handler needs about the request it's answering beyond the
 * request body itself. Introduced in M3 (PRD §7.3) alongside Fetch
 * long-polling: before this, every handler always returned its response
 * body synchronously from {@code handle()}, and RequestDispatcher alone
 * knew the correlation ID / header version / connection needed to wrap and
 * send it. A parked Fetch has to send its answer later, from outside that
 * call stack entirely — {@link #sendAsync} lets it do that using the exact
 * same wrapping RequestDispatcher itself uses, so a deferred response is
 * byte-identical in shape to an immediate one.
 */
public record RequestContext(short apiVersion, int correlationId, short responseHeaderVersion, Connection connection) {

    public void sendAsync(byte[] body) {
        connection.enqueueResponse(ResponseFramer.frame(correlationId, responseHeaderVersion, body));
    }
}
