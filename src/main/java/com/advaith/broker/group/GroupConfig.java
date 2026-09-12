package com.advaith.broker.group;

/**
 * M3 consumer-group tuning (PRD §7.6), plus one value the PRD's config
 * list doesn't name explicitly: {@code initialRebalanceDelayMs} mirrors
 * real Kafka's own {@code group.initial.rebalance.delay.ms} — the window
 * a brand-new group's first JoinGroup waits before finalizing, deliberately
 * short, specifically to let a second consumer starting moments later join
 * the SAME initial rebalance instead of forcing an immediate second one.
 * Without it, two consumers started close together but not simultaneously
 * would each get their own single-member generation, defeating the whole
 * point of a group.
 */
public record GroupConfig(
        int minSessionTimeoutMs,
        int maxSessionTimeoutMs,
        int rebalanceTimeoutCeilingMs,
        int initialRebalanceDelayMs) {
}
