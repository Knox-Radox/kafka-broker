package com.advaith.broker.metrics;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Basic correctness of the push-side registry — counters, error labels, and the LatencyWindow percentile math (PRD §9.4). */
class MetricsTest {

    @Test
    void requestCountsAccumulatePerApi() {
        Metrics metrics = new Metrics();
        metrics.incrementRequests("Produce");
        metrics.incrementRequests("Produce");
        metrics.incrementRequests("Fetch");

        Map<String, Long> counts = metrics.requestCountsSnapshot();
        assertEquals(2L, counts.get("Produce"));
        assertEquals(1L, counts.get("Fetch"));
    }

    @Test
    void errorCountsAreDistinctPerApiAndReason() {
        Metrics metrics = new Metrics();
        metrics.incrementErrors("Produce", "NOT_LEADER_OR_FOLLOWER");
        metrics.incrementErrors("Produce", "NOT_LEADER_OR_FOLLOWER");
        metrics.incrementErrors("Fetch", "NOT_LEADER_OR_FOLLOWER");

        Map<Metrics.ErrorKey, Long> counts = metrics.errorCountsSnapshot();
        assertEquals(2L, counts.get(new Metrics.ErrorKey("Produce", "NOT_LEADER_OR_FOLLOWER")));
        assertEquals(1L, counts.get(new Metrics.ErrorKey("Fetch", "NOT_LEADER_OR_FOLLOWER")));
    }

    @Test
    void latencyPercentilesReflectRecordedSamples() {
        Metrics metrics = new Metrics();
        // 1..100 ms in 1ms steps: p50 should land near 50ms, p99 near 99ms.
        for (int ms = 1; ms <= 100; ms++) {
            metrics.recordLatencyNanos("Produce", ms * 1_000_000L);
        }

        LatencyWindow.Percentiles percentiles = metrics.requestLatenciesSnapshot().get("Produce");
        assertEquals(100L, percentiles.count());
        assertTrue(percentiles.p50Seconds() >= 0.049 && percentiles.p50Seconds() <= 0.051,
                "expected p50 near 50ms, got " + percentiles.p50Seconds());
        assertTrue(percentiles.p99Seconds() >= 0.098 && percentiles.p99Seconds() <= 0.100,
                "expected p99 near 99ms, got " + percentiles.p99Seconds());
        // sum of 1..100 ms = 5050ms = 5.05s
        assertEquals(5.05, percentiles.sumSeconds(), 0.001);
    }

    @Test
    void unknownApiHasNoRecordedLatency() {
        Metrics metrics = new Metrics();
        assertTrue(metrics.requestLatenciesSnapshot().isEmpty());
    }
}
