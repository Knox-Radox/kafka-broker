package com.advaith.broker.log;

/**
 * M2 storage tuning (PRD §6.7). One design decision worth calling out:
 * flushIntervalMessages=1 is the default — fsync every record — trading
 * throughput for the strongest durability M2 can offer, specifically so
 * the fsync-cost benchmark (§6.6) has an honest, safety-first baseline to
 * measure a batched policy against, rather than starting from "fast" and
 * discovering the cost of "safe" only when asked.
 */
public record StorageConfig(
        long segmentBytes,
        int indexIntervalBytes,
        long retentionBytes,
        long retentionMs,
        int flushIntervalMessages,
        long flushIntervalMs) {

    public static final long UNLIMITED = -1;
}
