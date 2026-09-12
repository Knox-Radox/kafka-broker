package com.advaith.broker.replication;

import java.util.ArrayList;
import java.util.List;

/**
 * Which brokers hold which partition, computed the same way by every
 * broker independently with zero coordination (PRD §8.1's "no controller,
 * no quorum" decision applies here too — this project never had an
 * auto-topic-creation or admin-API controller to assign replicas even
 * before M4, so replica assignment has to stay in that same "static,
 * derivable from config alone" family as topics (§5.1) and peers (§8.2)).
 *
 * Design: round-robin over the sorted, cluster-wide broker ID list,
 * rotating the starting point by partition index — the same shape real
 * Kafka's own default assignor produces for a fresh topic, just computed
 * on demand instead of persisted by a controller. Every broker in the
 * cluster is constructed with the identical (self + every configured
 * peer) broker ID set, so this function returns the exact same list on
 * every broker for the same (topic, partition) — no gossip needed to
 * agree on it. The FIRST id in the returned list is the initial leader.
 */
public final class ReplicaAssignment {

    private ReplicaAssignment() {}

    public static List<Integer> replicasFor(String topic, int partition, List<Integer> sortedBrokerIds, int replicationFactor) {
        int n = sortedBrokerIds.size();
        int factor = Math.min(replicationFactor, n);
        // Same rotation idea real Kafka's own round-robin assignor uses:
        // spread a topic's partitions' leaders evenly across brokers by
        // starting each partition's replica list at a different broker,
        // rather than always favoring broker 0. Hashing in the topic name
        // (not just the partition index) keeps two different topics with
        // the same partition count from all picking identical leaders.
        int start = Math.floorMod(topic.hashCode() + partition, n);
        List<Integer> replicas = new ArrayList<>(factor);
        for (int i = 0; i < factor; i++) {
            replicas.add(sortedBrokerIds.get((start + i) % n));
        }
        return replicas;
    }
}
