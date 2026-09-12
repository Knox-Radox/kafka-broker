package com.advaith.broker.metrics;

import com.advaith.broker.log.LogManager;
import com.advaith.broker.log.PartitionLog;
import com.advaith.broker.replication.ReplicaManager;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

/**
 * Pulls a live snapshot from {@link Metrics} (push-side counters/latencies)
 * and from {@code LogManager}/{@code ReplicaManager} (pull-side gauges —
 * see below for why those are computed fresh here rather than pushed) and
 * renders it as Prometheus text via {@link PrometheusTextFormat}. Pure
 * wiring, deliberately not unit tested itself (§9.7 explicitly calls out
 * {@code PrometheusTextFormat} as the piece worth testing in isolation;
 * nothing here is logic beyond "call three things and concatenate" —
 * verified for real instead, against a running broker, per this
 * milestone's own §9.7 rule that its deliverables largely ARE the tests).
 *
 * <p>Per-partition gauges (high-water mark, log-end-offset, ISR size) are
 * computed fresh on every scrape rather than updated by a push from
 * ProduceHandler/ReplicaManager on every write: they're already cheap,
 * synchronous queries ({@code ReplicaManager.highWaterMark}, {@code
 * PartitionLog.logEndOffset}, {@code ReplicaManager.isrFor}) that every
 * client-facing handler already calls per-request, so there's no
 * bookkeeping to duplicate — a scrape is just "ask the same questions a
 * real request would, for every partition, right now."
 */
public final class MetricsExporter {

    private final Metrics metrics;
    private final LogManager logManager;
    private final ReplicaManager replicaManager;

    public MetricsExporter(Metrics metrics, LogManager logManager, ReplicaManager replicaManager) {
        this.metrics = metrics;
        this.logManager = logManager;
        this.replicaManager = replicaManager;
    }

    public String render() {
        StringBuilder out = new StringBuilder();
        writeRequestCounts(out);
        writeErrorCounts(out);
        writeLatencies(out);
        writePartitionGauges(out);
        return out.toString();
    }

    private void writeRequestCounts(StringBuilder out) {
        List<PrometheusTextFormat.CounterSample> samples = new ArrayList<>();
        metrics.requestCountsSnapshot().forEach((api, count) ->
                samples.add(new PrometheusTextFormat.CounterSample(PrometheusTextFormat.Labels.of("api", api), count)));
        samples.sort((a, b) -> a.labels().render().compareTo(b.labels().render()));
        PrometheusTextFormat.writeCounter(out, "broker_requests_total", "Total requests received, by API.", samples);
    }

    private void writeErrorCounts(StringBuilder out) {
        List<PrometheusTextFormat.CounterSample> samples = new ArrayList<>();
        metrics.errorCountsSnapshot().forEach((key, count) ->
                samples.add(new PrometheusTextFormat.CounterSample(
                        PrometheusTextFormat.Labels.of("api", key.api(), "error", key.errorLabel()), count)));
        samples.sort((a, b) -> a.labels().render().compareTo(b.labels().render()));
        PrometheusTextFormat.writeCounter(out, "broker_errors_total", "Total requests that ended in an error, by API and reason.", samples);
    }

    private void writeLatencies(StringBuilder out) {
        List<PrometheusTextFormat.SummarySample> samples = new ArrayList<>();
        metrics.requestLatenciesSnapshot().forEach((api, percentiles) ->
                samples.add(new PrometheusTextFormat.SummarySample(
                        PrometheusTextFormat.Labels.of("api", api),
                        percentiles.p50Seconds(), percentiles.p95Seconds(), percentiles.p99Seconds(),
                        percentiles.sumSeconds(), percentiles.count())));
        samples.sort((a, b) -> a.labels().render().compareTo(b.labels().render()));
        PrometheusTextFormat.writeSummary(out, "broker_request_latency_seconds",
                "Time spent in a request's handler, in seconds. Quantiles are over a recent window (see LatencyWindow); _sum/_count are all-time.",
                samples);
    }

    /** PRD §9.4: HWM/LEO per partition (replication-lag visibility), and ISR size (the single most direct "something's wrong" signal). */
    private void writePartitionGauges(StringBuilder out) {
        List<PrometheusTextFormat.GaugeSample> hwm = new ArrayList<>();
        List<PrometheusTextFormat.GaugeSample> leo = new ArrayList<>();
        List<PrometheusTextFormat.GaugeSample> isrSize = new ArrayList<>();

        for (String topic : new TreeSet<>(logManager.topicNames())) {
            int partitionCount = logManager.partitionCount(topic);
            for (int partition = 0; partition < partitionCount; partition++) {
                PrometheusTextFormat.Labels labels = PrometheusTextFormat.Labels.of(
                        "topic", topic, "partition", Integer.toString(partition));
                PartitionLog log = logManager.getPartition(topic, partition).orElse(null);
                if (log == null) {
                    continue;
                }
                leo.add(new PrometheusTextFormat.GaugeSample(labels, log.logEndOffset()));
                hwm.add(new PrometheusTextFormat.GaugeSample(labels, replicaManager.highWaterMark(topic, partition)));
                isrSize.add(new PrometheusTextFormat.GaugeSample(labels, replicaManager.isrFor(topic, partition).size()));
            }
        }

        PrometheusTextFormat.writeGauge(out, "broker_high_water_mark", "The high-water mark (last offset safely readable by a normal consumer) per partition.", hwm);
        PrometheusTextFormat.writeGauge(out, "broker_log_end_offset", "The raw log end offset (next offset that would be assigned) per partition.", leo);
        PrometheusTextFormat.writeGauge(out, "broker_isr_size", "Current in-sync replica count per partition — a shrinking value is the most direct live sign something is wrong.", isrSize);
    }
}
