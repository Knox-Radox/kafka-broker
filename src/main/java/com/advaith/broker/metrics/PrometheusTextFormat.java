package com.advaith.broker.metrics;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Hand-rolled writer for Prometheus's plain-text exposition format (PRD
 * §9.1's scope decision: no client library dependency). Deliberately
 * decoupled from {@link Metrics}/{@code LogManager}/{@code
 * ReplicaManager} — it only ever sees already-collected values — so
 * §9.7's "assert the exact output format" test can check this class alone
 * without booting a broker.
 *
 * <p>The format itself (https://prometheus.io/docs/instrumenting/exposition_formats/,
 * the older but still-accepted 0.0.4 text version): each metric FAMILY
 * (one name, possibly many label combinations) gets exactly one {@code #
 * HELP} line and one {@code # TYPE} line, followed by one sample line per
 * label combination: {@code name{label="value",...} number}. A malformed
 * line here breaks scraping silently on both ends — no error on the
 * broker, no error from Prometheus, just a metric that's quietly absent —
 * which is why this gets its own test rather than trusting manual
 * inspection (the same lesson as M1's ListOffsets schema mistake,
 * see JOURNAL.md).
 */
public final class PrometheusTextFormat {

    private PrometheusTextFormat() {}

    /** An ordered set of label names/values. Order is preserved exactly as given — callers pass a fixed order, so output is deterministic without needing to sort. */
    public static final class Labels {
        static final Labels NONE = new Labels(Map.of());
        private final Map<String, String> byKey;

        private Labels(Map<String, String> byKey) {
            this.byKey = byKey;
        }

        public static Labels of(String... keyValuePairs) {
            if (keyValuePairs.length % 2 != 0) {
                throw new IllegalArgumentException("labels must be given as key,value pairs");
            }
            Map<String, String> map = new LinkedHashMap<>();
            for (int i = 0; i < keyValuePairs.length; i += 2) {
                map.put(keyValuePairs[i], keyValuePairs[i + 1]);
            }
            return new Labels(map);
        }

        String render() {
            if (byKey.isEmpty()) {
                return "";
            }
            StringBuilder sb = new StringBuilder("{");
            boolean first = true;
            for (Map.Entry<String, String> entry : byKey.entrySet()) {
                if (!first) {
                    sb.append(',');
                }
                first = false;
                sb.append(entry.getKey()).append("=\"").append(escape(entry.getValue())).append('"');
            }
            return sb.append('}').toString();
        }

        /** Prometheus's own escaping rule for a label value: backslash and quote are escaped, a literal newline becomes \n. */
        private static String escape(String value) {
            return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
        }
    }

    public record CounterSample(Labels labels, long value) {}

    public record GaugeSample(Labels labels, double value) {}

    /** Renders one counter metric family: HELP, TYPE, then one line per sample. */
    public static void writeCounter(StringBuilder out, String name, String help, List<CounterSample> samples) {
        out.append("# HELP ").append(name).append(' ').append(help).append('\n');
        out.append("# TYPE ").append(name).append(" counter\n");
        for (CounterSample sample : samples) {
            out.append(name).append(sample.labels().render()).append(' ').append(sample.value()).append('\n');
        }
    }

    /** Renders one gauge metric family: HELP, TYPE, then one line per sample. */
    public static void writeGauge(StringBuilder out, String name, String help, List<GaugeSample> samples) {
        out.append("# HELP ").append(name).append(' ').append(help).append('\n');
        out.append("# TYPE ").append(name).append(" gauge\n");
        for (GaugeSample sample : samples) {
            out.append(name).append(sample.labels().render()).append(' ').append(formatDouble(sample.value())).append('\n');
        }
    }

    public record SummarySample(Labels labels, double p50, double p95, double p99, double sum, long count) {}

    /**
     * Renders one Summary metric family in the same shape Prometheus's own
     * client libraries emit: {@code quantile}-labelled lines for the
     * metric name itself, then a {@code _sum} and {@code _count} line per
     * label combination (the cumulative totals — see LatencyWindow's
     * javadoc for why these are all-time while the quantiles above them
     * are only a recent window, which is the documented, correct-by-design
     * Summary convention this mirrors).
     */
    public static void writeSummary(StringBuilder out, String name, String help, List<SummarySample> samples) {
        out.append("# HELP ").append(name).append(' ').append(help).append('\n');
        out.append("# TYPE ").append(name).append(" summary\n");
        for (SummarySample sample : samples) {
            writeQuantileLine(out, name, sample.labels(), "0.5", sample.p50());
            writeQuantileLine(out, name, sample.labels(), "0.95", sample.p95());
            writeQuantileLine(out, name, sample.labels(), "0.99", sample.p99());
            out.append(name).append("_sum").append(sample.labels().render()).append(' ').append(formatDouble(sample.sum())).append('\n');
            out.append(name).append("_count").append(sample.labels().render()).append(' ').append(sample.count()).append('\n');
        }
    }

    private static void writeQuantileLine(StringBuilder out, String name, Labels labels, String quantile, double value) {
        Map<String, String> withQuantile = new LinkedHashMap<>(labels.byKey);
        withQuantile.put("quantile", quantile);
        out.append(name).append(new Labels(withQuantile).render()).append(' ').append(formatDouble(value)).append('\n');
    }

    /** Whole numbers render without a trailing ".0" (matches real exporters' style, and keeps offset/count gauges readable as plain integers); everything else uses Double.toString. */
    private static String formatDouble(double value) {
        if (!Double.isInfinite(value) && !Double.isNaN(value) && value == Math.rint(value) && Math.abs(value) < 1e15) {
            return Long.toString((long) value);
        }
        return Double.toString(value);
    }
}
