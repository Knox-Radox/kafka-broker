package com.advaith.broker.metrics;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * PRD §9.7's explicitly-named test: a malformed exposition line breaks
 * scraping silently on both ends (no error on the broker, no error from
 * Prometheus — just a metric that quietly never shows up), so the exact
 * text this writer produces is worth pinning down byte-for-byte rather
 * than trusting manual inspection, the same lesson M1's ListOffsets
 * schema mistake already taught (see JOURNAL.md, 2026-09-11).
 */
class PrometheusTextFormatTest {

    @Test
    void writesACounterFamilyInExactPrometheusFormat() {
        StringBuilder out = new StringBuilder();
        PrometheusTextFormat.writeCounter(out, "broker_requests_total", "Total requests received, by API.", List.of(
                new PrometheusTextFormat.CounterSample(PrometheusTextFormat.Labels.of("api", "Produce"), 42L),
                new PrometheusTextFormat.CounterSample(PrometheusTextFormat.Labels.of("api", "Fetch"), 7L)));

        assertEquals(
                "# HELP broker_requests_total Total requests received, by API.\n" +
                "# TYPE broker_requests_total counter\n" +
                "broker_requests_total{api=\"Produce\"} 42\n" +
                "broker_requests_total{api=\"Fetch\"} 7\n",
                out.toString());
    }

    @Test
    void writesAGaugeFamilyWithMultipleLabelsInExactPrometheusFormat() {
        StringBuilder out = new StringBuilder();
        PrometheusTextFormat.writeGauge(out, "broker_high_water_mark", "The high-water mark per partition.", List.of(
                new PrometheusTextFormat.GaugeSample(PrometheusTextFormat.Labels.of("topic", "orders", "partition", "0"), 128.0)));

        assertEquals(
                "# HELP broker_high_water_mark The high-water mark per partition.\n" +
                "# TYPE broker_high_water_mark gauge\n" +
                "broker_high_water_mark{topic=\"orders\",partition=\"0\"} 128\n",
                out.toString());
    }

    @Test
    void gaugeWithNoSamplesStillWritesHelpAndTypeLinesButNoSampleLines() {
        StringBuilder out = new StringBuilder();
        PrometheusTextFormat.writeGauge(out, "broker_isr_size", "ISR size per partition.", List.of());

        assertEquals(
                "# HELP broker_isr_size ISR size per partition.\n" +
                "# TYPE broker_isr_size gauge\n",
                out.toString());
    }

    @Test
    void writesASummaryFamilyWithQuantileSumAndCountLines() {
        StringBuilder out = new StringBuilder();
        PrometheusTextFormat.writeSummary(out, "broker_request_latency_seconds", "Request latency, in seconds.", List.of(
                new PrometheusTextFormat.SummarySample(
                        PrometheusTextFormat.Labels.of("api", "Produce"),
                        0.001, 0.004, 0.009, 12.5, 1000L)));

        assertEquals(
                "# HELP broker_request_latency_seconds Request latency, in seconds.\n" +
                "# TYPE broker_request_latency_seconds summary\n" +
                "broker_request_latency_seconds{api=\"Produce\",quantile=\"0.5\"} 0.001\n" +
                "broker_request_latency_seconds{api=\"Produce\",quantile=\"0.95\"} 0.004\n" +
                "broker_request_latency_seconds{api=\"Produce\",quantile=\"0.99\"} 0.009\n" +
                "broker_request_latency_seconds_sum{api=\"Produce\"} 12.5\n" +
                "broker_request_latency_seconds_count{api=\"Produce\"} 1000\n",
                out.toString());
    }

    @Test
    void escapesBackslashesAndQuotesInLabelValues() {
        StringBuilder out = new StringBuilder();
        PrometheusTextFormat.writeCounter(out, "m", "h", List.of(
                new PrometheusTextFormat.CounterSample(PrometheusTextFormat.Labels.of("k", "back\\slash \"quoted\""), 1L)));

        assertEquals(
                "# HELP m h\n" +
                "# TYPE m counter\n" +
                "m{k=\"back\\\\slash \\\"quoted\\\"\"} 1\n",
                out.toString());
    }
}
