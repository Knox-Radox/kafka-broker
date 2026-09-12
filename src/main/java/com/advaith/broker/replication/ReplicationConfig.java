package com.advaith.broker.replication;

import java.util.List;

/**
 * M4 cluster/replication tuning (PRD §8.7). An empty {@code peers} list
 * (the M1-M3 single-broker default) makes every partition trivially
 * self-led with an ISR of just this broker — see ReplicaManager — so
 * nothing above this layer needs a separate "replication is off" code
 * path; single-broker mode is just the degenerate case of the general
 * mechanism, the same way FetchCompletionListener.NONE and
 * SelectorTicker.NONE are degenerate no-op cases of their interfaces.
 */
public record ReplicationConfig(
        List<PeerInfo> peers,
        int replicationFactor,
        long replicaLagTimeMaxMs,
        long leaderLeaseRenewIntervalMs,
        long leaderLeaseTimeoutMs,
        int replicaFetchMaxWaitMs,
        int replicaFetchMinBytes) {

    public static ReplicationConfig singleBrokerDefault() {
        return new ReplicationConfig(List.of(), 1, 10_000, 2_000, 6_000, 500, 1);
    }
}
