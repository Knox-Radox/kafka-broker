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
     * @param request positioned immediately after the request header
     * @return the encoded response body, or {@code null} to send no
     *         response at all — the only defined case for this in M1 is
     *         Produce with {@code acks=0} (PRD §5.3).
     */
    byte[] handle(short apiVersion, ProtocolReader request);
}
