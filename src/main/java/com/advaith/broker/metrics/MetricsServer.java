package com.advaith.broker.metrics;

import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

/**
 * PRD §9.1's explicit scope decision: metrics are served via the JDK's own
 * {@code com.sun.net.httpserver.HttpServer} (ships with the JDK, not an
 * external dependency — see STUDY_GUIDE.md's M5 metrics gate for the full
 * rationale), on a small, separate port from the client-facing one.
 *
 * <p>Deliberately its own {@code HttpServer} instance with its own thread
 * pool — never sharing {@code NetworkServer}'s single selector thread. A
 * slow or wedged Prometheus scrape (or a network partition to whoever is
 * scraping) must never be able to add so much as a millisecond of latency
 * to a real client's Produce/Fetch — the exact same "a dead/slow peer must
 * not stall client-facing traffic" principle PeerReplicator's javadoc
 * already established for replication in M4, applied again here for a
 * different kind of peer.
 */
public final class MetricsServer {

    private static final Logger log = LoggerFactory.getLogger(MetricsServer.class);

    private final HttpServer httpServer;

    public MetricsServer(int port, MetricsExporter exporter) throws IOException {
        httpServer = HttpServer.create(new InetSocketAddress(port), 0);
        httpServer.createContext("/metrics", exchange -> {
            try {
                byte[] body = exporter.render().getBytes(StandardCharsets.UTF_8);
                // The exact content-type real Prometheus's own exporters use
                // for the text format (version=0.0.4) — a scraper doesn't
                // strictly require it, but a real Prometheus server logs a
                // warning without it, and getting a wire format's headers
                // right instead of "close enough" is this whole project's
                // habit (see JOURNAL.md's ListOffsets/ApiVersions entries).
                exchange.getResponseHeaders().add("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
                exchange.sendResponseHeaders(200, body.length);
                try (OutputStream responseBody = exchange.getResponseBody()) {
                    responseBody.write(body);
                }
            } catch (RuntimeException e) {
                // Same defence-in-depth principle as NetworkServer's own
                // per-connection catch: a bug rendering one scrape must not
                // take the metrics server itself down for every future one.
                log.warn("failed to render /metrics response", e);
                exchange.sendResponseHeaders(500, -1);
            } finally {
                exchange.close();
            }
        });
        httpServer.setExecutor(null); // default: a new thread per exchange — fine at the scrape rates this endpoint will ever see
    }

    public void start() {
        httpServer.start();
        log.info("metrics endpoint listening on port {}", httpServer.getAddress().getPort());
    }

    public void stop() {
        httpServer.stop(0);
    }
}
