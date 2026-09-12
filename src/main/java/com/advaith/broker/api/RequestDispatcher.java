package com.advaith.broker.api;

import com.advaith.broker.network.FrameHandler;
import com.advaith.broker.network.RequestFrame;
import com.advaith.broker.protocol.ApiKey;
import com.advaith.broker.protocol.HeaderVersions;
import com.advaith.broker.protocol.ProtocolReader;
import com.advaith.broker.protocol.RequestHeader;
import com.advaith.broker.protocol.ResponseFramer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        short responseHeaderVersion = HeaderVersions.lookup(header.apiKey(), header.apiVersion())
                .responseHeaderVersion();
        RequestContext context = new RequestContext(
                header.apiVersion(), header.correlationId(), responseHeaderVersion, frame.connection());

        byte[] responseBody;
        try {
            responseBody = handler.handle(context, reader);
        } catch (RuntimeException e) {
            log.warn("handler for {} failed on request from {}, closing connection",
                    handler.apiKey(), frame.connection().remoteAddress(), e);
            frame.connection().close();
            return;
        }

        if (responseBody == null) {
            // Either genuinely no response is ever due (Produce acks=0), or
            // the handler already sent its own via context.sendAsync() —
            // e.g. an immediately-satisfiable Fetch still returns bytes
            // here normally, but one it had to park (PRD §7.3) answers
            // later, from outside this call, using the identical framing
            // ResponseFramer gives both paths.
            return;
        }

        frame.connection().enqueueResponse(ResponseFramer.frame(header.correlationId(), responseHeaderVersion, responseBody));
    }
}
