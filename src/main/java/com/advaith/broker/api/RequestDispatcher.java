package com.advaith.broker.api;

import com.advaith.broker.metrics.Metrics;
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
 *
 * <p>Since M5 (PRD §9.4), this is also the one place every request and
 * every handler-visible failure passes through regardless of which of the
 * twelve APIs it is — which is exactly what makes it the natural home for
 * the GENERIC half of this project's metrics (request counts, latency, and
 * the failures visible at this layer: malformed headers, unsupported
 * versions, handler exceptions). Full per-wire-error-code labels (e.g.
 * {@code error="UNKNOWN_TOPIC_OR_PARTITION"}) are a deliberate scope cut
 * NOT done here — that would require this layer to decode every API's own
 * response schema, duplicating knowledge each handler already owns, for
 * value that matters most only for Produce/Fetch (which is what the M5
 * chaos test and benchmark actually watch, via the HWM/LEO/ISR gauges
 * MetricsExporter reads directly from LogManager/ReplicaManager instead).
 * See STUDY_GUIDE.md's M5 metrics gate for the full writeup.
 */
public final class RequestDispatcher implements FrameHandler {

    private static final Logger log = LoggerFactory.getLogger(RequestDispatcher.class);

    /** api label used for failures that happen before a handler is even identified (malformed header, unknown api key) — there is no ApiKey to name yet. */
    private static final String UNKNOWN_API = "Unknown";

    private final Map<Integer, ApiHandler> handlersByApiKey;
    private final Metrics metrics;

    public RequestDispatcher(List<ApiHandler> handlers, Metrics metrics) {
        Map<Integer, ApiHandler> map = new HashMap<>();
        for (ApiHandler handler : handlers) {
            map.put(handler.apiKey().key, handler);
        }
        this.handlersByApiKey = Map.copyOf(map);
        this.metrics = metrics;
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
            metrics.incrementErrors(UNKNOWN_API, "MALFORMED_HEADER");
            frame.connection().close();
            return;
        }

        ApiHandler handler = handlersByApiKey.get((int) header.apiKey());
        if (handler == null) {
            log.warn("unsupported api_key {} from {}, closing connection",
                    header.apiKey(), frame.connection().remoteAddress());
            metrics.incrementErrors(UNKNOWN_API, "UNSUPPORTED_API_KEY");
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
            metrics.incrementErrors(handler.apiKey().displayName, "UNSUPPORTED_VERSION");
            frame.connection().close();
            return;
        }

        short responseHeaderVersion = HeaderVersions.lookup(header.apiKey(), header.apiVersion())
                .responseHeaderVersion();
        RequestContext context = new RequestContext(
                header.apiVersion(), header.correlationId(), responseHeaderVersion, frame.connection());

        metrics.incrementRequests(handler.apiKey().displayName);
        long startNanos = System.nanoTime();
        byte[] responseBody;
        try {
            responseBody = handler.handle(context, reader);
        } catch (RuntimeException e) {
            log.warn("handler for {} failed on request from {}, closing connection",
                    handler.apiKey(), frame.connection().remoteAddress(), e);
            metrics.incrementErrors(handler.apiKey().displayName, "HANDLER_EXCEPTION");
            frame.connection().close();
            return;
        }
        // Measures time spent on the selector thread inside handle() itself
        // — correct and complete for every synchronous API, but an
        // UNDERCOUNT for a request a handler parks and answers later
        // (Fetch long-polling, an acks=-1 Produce waiting on the HWM, PRD
        // §7.3/§8.6): those return from handle() almost immediately, so
        // this metric only ever reflects "how long the dispatch itself
        // took," not "how long the client actually waited." §9.3's
        // benchmark tool measures the latter independently, from the
        // client's own wall clock, and that number — not this one — is
        // the authoritative one for BENCHMARKS.md.
        metrics.recordLatencyNanos(handler.apiKey().displayName, System.nanoTime() - startNanos);

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
