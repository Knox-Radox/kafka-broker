package com.advaith.broker.replication;

/**
 * Notified whenever a partition's high-water mark might have moved
 * forward (PRD §8.6) — the replication-side counterpart of
 * FetchCompletionListener (PRD §7.3): ProduceHandler parks an acks=-1
 * request until the HWM reaches the offset it just appended, and needs to
 * be woken the instant that becomes true instead of waiting for the next
 * timer sweep, the same reasoning §7.3 already established for Fetch
 * long-polling.
 */
public interface HwmAdvancedListener {

    HwmAdvancedListener NONE = (topic, partition, highWaterMark) -> { };

    void onHighWaterMarkAdvanced(String topic, int partition, long highWaterMark);
}
