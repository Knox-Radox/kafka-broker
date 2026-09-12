package com.advaith.broker.metrics;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * The broker-wide, in-memory metrics registry (PRD §9.4): the push side
 * only. Nothing here knows about Prometheus's text format ({@link
 * PrometheusTextFormat} owns that, unit-tested in isolation per §9.7) or
 * about {@code LogManager}/{@code ReplicaManager} ({@link MetricsExporter}
 * owns pulling those) — this class is just three concurrent maps, safe to
 * write from the single selector thread (every client request) and read
 * from the metrics HTTP server's own thread (every scrape) at the same
 * time, with no coordination between the two beyond what
 * {@code ConcurrentHashMap}/{@code LongAdder} already give for free.
 *
 * <p>Deliberately keyed by plain {@code String} labels rather than {@link
 * com.advaith.broker.protocol.ApiKey} — this package has no reason to
 * depend on the protocol package at all; the caller (RequestDispatcher)
 * already has an ApiKey in hand and passes {@code apiKey().displayName}
 * through as a string, the same string that ends up in the exported
 * Prometheus label verbatim.
 */
public final class Metrics {

    /** Distinguishes {@code broker_errors_total} samples that share an API but not an error reason. */
    public record ErrorKey(String api, String errorLabel) {}

    private final ConcurrentHashMap<String, LongAdder> requestCounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ErrorKey, LongAdder> errorCounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LatencyWindow> requestLatencies = new ConcurrentHashMap<>();

    public void incrementRequests(String api) {
        requestCounts.computeIfAbsent(api, k -> new LongAdder()).increment();
    }

    public void incrementErrors(String api, String errorLabel) {
        errorCounts.computeIfAbsent(new ErrorKey(api, errorLabel), k -> new LongAdder()).increment();
    }

    public void recordLatencyNanos(String api, long nanos) {
        requestLatencies.computeIfAbsent(api, k -> new LatencyWindow()).record(nanos);
    }

    // ==================== read side, used only by MetricsExporter at scrape time ====================

    public Map<String, Long> requestCountsSnapshot() {
        Map<String, Long> out = new LinkedHashMap<>();
        requestCounts.forEach((api, adder) -> out.put(api, adder.sum()));
        return out;
    }

    public Map<ErrorKey, Long> errorCountsSnapshot() {
        Map<ErrorKey, Long> out = new LinkedHashMap<>();
        errorCounts.forEach((key, adder) -> out.put(key, adder.sum()));
        return out;
    }

    Map<String, LatencyWindow.Percentiles> requestLatenciesSnapshot() {
        Map<String, LatencyWindow.Percentiles> out = new LinkedHashMap<>();
        requestLatencies.forEach((api, window) -> out.put(api, window.snapshot()));
        return out;
    }
}
