package com.advaith.broker.api;

import com.advaith.broker.protocol.ApiKey;
import com.advaith.broker.protocol.ProtocolReader;

/**
 * One implementation per Kafka API. A handler only ever sees its own
 * request body (the header has already been parsed and stripped by
 * RequestDispatcher) and returns only its own response body — the response
 * header and length prefix are identical machinery for every API, so they
 * live in the dispatcher instead of being duplicated five times here.
 */
public interface ApiHandler {

    ApiKey apiKey();

    /**
     * @param context correlation ID / header version / connection for this
     *                 request — a handler only needs this if it must send
     *                 its own response later instead of returning it here
     *                 (see {@link RequestContext#sendAsync}; Fetch
     *                 long-polling, PRD §7.3, is the first and so far only
     *                 handler that does)
     * @param request  positioned immediately after the request header
     * @return the encoded response body, or {@code null} to send no
     *         response through the normal synchronous path — either
     *         because none is ever due (Produce with {@code acks=0}, PRD
     *         §5.3), or because the handler already sent one itself via
     *         {@code context.sendAsync(...)} and there is nothing left for
     *         RequestDispatcher to do.
     */
    byte[] handle(RequestContext context, ProtocolReader request);
}
