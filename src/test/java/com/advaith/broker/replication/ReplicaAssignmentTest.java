package com.advaith.broker.replication;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReplicaAssignmentTest {

    @Test
    void everyBrokerComputesTheIdenticalAssignmentIndependently() {
        List<Integer> cluster = List.of(0, 1, 2);
        // The whole point of static, derivable assignment (PRD §8.1: no
        // controller) is that nobody needs to ask anybody else — calling
        // this twice with the same inputs, standing in for two different
        // broker processes, must agree byte-for-byte.
        assertEquals(
                ReplicaAssignment.replicasFor("orders", 2, cluster, 3),
                ReplicaAssignment.replicasFor("orders", 2, cluster, 3));
    }

    @Test
    void replicationFactorIsCappedAtClusterSize() {
        List<Integer> cluster = List.of(0, 1);
        List<Integer> replicas = ReplicaAssignment.replicasFor("test", 0, cluster, 5);
        assertEquals(2, replicas.size()); // can't replicate onto more brokers than exist
    }

    @Test
    void everyReplicaIsDistinctAndFromTheClusterSet() {
        List<Integer> cluster = List.of(0, 1, 2, 3);
        List<Integer> replicas = ReplicaAssignment.replicasFor("orders", 3, cluster, 3);
        assertEquals(3, replicas.size());
        assertEquals(3, replicas.stream().distinct().count());
        assertTrue(cluster.containsAll(replicas));
    }

    @Test
    void differentPartitionsSpreadLeadershipAcrossBrokers() {
        List<Integer> cluster = List.of(0, 1, 2);
        // With as many partitions as brokers, round-robin should hand out
        // every broker as SOME partition's first (leader) replica — a
        // fixed "partition 0 always leads with broker 0" rule would fail
        // this trivially.
        var leaders = new java.util.HashSet<Integer>();
        for (int p = 0; p < 3; p++) {
            leaders.add(ReplicaAssignment.replicasFor("orders", p, cluster, 1).get(0));
        }
        assertEquals(3, leaders.size());
    }
}
