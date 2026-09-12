package com.advaith.broker.metrics;

import java.util.Arrays;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;

/**
 * PRD §9.4 offers a choice: a full histogram type, or "at minimum
 * pre-computed percentile gauges... note which choice was made and why"
 * (see STUDY_GUIDE.md's M5 metrics gate for the full writeup). This is a
 * real Prometheus <b>Summary</b>, not a Histogram: cumulative sum/count
 * since process start (two {@link LongAdder}s — correct forever, O(1) to
 * update) plus p50/p95/p99 computed by sorting a bounded ring buffer of
 * the most RECENT samples at scrape time. This mirrors exactly how
 * Prometheus's own official client libraries implement Summary (a
 * sliding-window quantile estimate alongside an all-time sum/count), not
 * a novel approximation invented for this project.
 *
 * <p>Tradeoff against a Histogram, made deliberately: quantiles here only
 * reflect the last {@link #CAPACITY} samples, not the whole process
 * lifetime, and computing them is O(n log n) at scrape time rather than
 * O(1). Both are irrelevant at this project's scrape frequency (a human
 * or a benchmark script hitting {@code /metrics} every few seconds, not
 * thousands of times a second) and request volume — and this is far
 * simpler than hand-rolling histogram bucket boundaries and interpolation,
 * which is the real complexity a Histogram would add for no benefit a
 * single-broker demo actually needs.
 *
 * <p>Package-private: {@link Metrics} is the only thing that constructs
 * one, one per (API, latency-kind) pair.
 */
final class LatencyWindow {

    private static final int CAPACITY = 2048;

    private final long[] ring = new long[CAPACITY];
    private int nextIndex = 0;
    private int filled = 0;
    /** Guards only the ring buffer — sum/count are lock-free LongAdders, correct even while a scrape is mid-sort. */
    private final ReentrantLock lock = new ReentrantLock();

    private final LongAdder sumNanos = new LongAdder();
    private final LongAdder count = new LongAdder();

    void record(long nanos) {
        sumNanos.add(nanos);
        count.increment();
        lock.lock();
        try {
            ring[nextIndex] = nanos;
            nextIndex = (nextIndex + 1) % CAPACITY;
            if (filled < CAPACITY) {
                filled++;
            }
        } finally {
            lock.unlock();
        }
    }

    record Percentiles(double p50Seconds, double p95Seconds, double p99Seconds, double sumSeconds, long count) {}

    Percentiles snapshot() {
        long[] copy;
        lock.lock();
        try {
            copy = Arrays.copyOf(ring, filled);
        } finally {
            lock.unlock();
        }
        Arrays.sort(copy);
        return new Percentiles(
                percentileOf(copy, 0.50),
                percentileOf(copy, 0.95),
                percentileOf(copy, 0.99),
                sumNanos.sum() / 1_000_000_000.0,
                count.sum());
    }

    private static double percentileOf(long[] sortedNanos, double p) {
        if (sortedNanos.length == 0) {
            return 0.0;
        }
        int index = (int) Math.ceil(p * sortedNanos.length) - 1;
        index = Math.max(0, Math.min(sortedNanos.length - 1, index));
        return sortedNanos[index] / 1_000_000_000.0;
    }
}
