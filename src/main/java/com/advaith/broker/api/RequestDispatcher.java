package com.advaith.broker.api;

import com.advaith.broker.network.FrameHandler;
import com.advaith.broker.network.RequestFrame;
import com.advaith.broker.protocol.ApiKey;
import com.advaith.broker.protocol.HeaderVersions;
import com.advaith.broker.protocol.ProtocolReader;
import com.advaith.broker.protocol.ProtocolWriter;
import com.advaith.broker.protocol.RequestHeader;
import com.advaith.broker.protocol.ResponseHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The "RequestDecoder" + "ApiHandler registry" boxes from PRD §4, combined:
 * turns a raw frame into (header, routed handler, response), and wraps
 * whatever the handler returns with the response header and length prefix
 * every API shares — that shared wrapping is exactly the logic PRD §4 warns
 * must not leak into the layers on either side of it.
 */
public final class RequestDispatcher implements FrameHandler {

    private static final Logger log = LoggerFactory.getLogger(RequestDispatcher.class);

    private final Map<Integer, ApiHandler> handlersByApiKey;

    public RequestDispatcher(List<ApiHandler> handlers) {
        Map<Integer, ApiHandler> map = new HashMap<>();
        for (ApiHandler handler : handlers) {
            map.put(handler.apiKey().key, handler);
        }
        this.handlersByApiKey = Map.copyOf(map);
    }

    @Override
    public void onFrame(RequestFrame frame) {
        ProtocolReader reader = new ProtocolReader(frame.payload());

        RequestHeader header;
        try {
            header = RequestHeader.parse(reader);
        } catch (RuntimeException e) {
            // A header we can't even parse is indistinguishable from a
            // malformed frame at this layer — same response as the network
            // layer's own malformed-length case (PRD acceptance criterion
            // 6): close this connection, leave every other one alone.
            log.warn("failed to parse request header from {}, closing connection: {}",
                    frame.connection().remoteAddress(), e.toString());
            frame.connection().close();
            return;
        }

        ApiHandler handler = handlersByApiKey.get((int) header.apiKey());
        if (handler == null) {
            log.warn("unsupported api_key {} from {}, closing connection",
                    header.apiKey(), frame.connection().remoteAddress());
            frame.connection().close();
            return;
        }

        // ApiVersions is the sole API defined to tolerate a version it
        // doesn't support (it IS the negotiation mechanism — see
        // ApiVersionsHandler). Every other API here advertises exactly one
        // version, so a spec-compliant client only ever sends that one;
        // anything else means the client skipped negotiation or has a bug,
        // and we have no version-specific schema to answer it correctly
        // with, so we treat it the same as a malformed request.
        boolean isApiVersions = handler.apiKey() == ApiKey.API_VERSIONS;
        if (!isApiVersions && !handler.apiKey().supportsVersion(header.apiVersion())) {
            log.warn("unsupported version {} of {} from {}, closing connection",
                    header.apiVersion(), handler.apiKey(), frame.connection().remoteAddress());
            frame.connection().close();
            return;
        }

        byte[] responseBody;
        try {
            responseBody = handler.handle(header.apiVersion(), reader);
        } catch (RuntimeException e) {
            log.warn("handler for {} failed on request from {}, closing connection",
                    handler.apiKey(), frame.connection().remoteAddress(), e);
            frame.connection().close();
            return;
        }

        if (responseBody == null) {
            return; // e.g. Produce acks=0: protocol defines no response at all
        }

        short responseHeaderVersion = HeaderVersions.lookup(header.apiKey(), header.apiVersion())
                .responseHeaderVersion();

        ProtocolWriter fullResponse = new ProtocolWriter(responseBody.length + 8);
        new ResponseHeader(header.correlationId()).write(fullResponse, responseHeaderVersion);
        fullResponse.writeRawBytes(responseBody);
        byte[] responseBytes = fullResponse.toByteArray();

        ByteBuffer framed = ByteBuffer.allocate(4 + responseBytes.length);
        framed.putInt(responseBytes.length);
        framed.put(responseBytes);
        framed.flip(); // switch to read mode: Connection.enqueueResponse only ever reads from this
        frame.connection().enqueueResponse(framed);
    }
}
