package com.advaith.broker.replication;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit coverage for §8.9's two explicitly-named cases: ISR membership at
 * the exact replica.lag.time.max.ms boundary, and epoch fencing. Package-
 * private class, so this test lives in the same package on purpose —
 * there is no public seam above it worth testing through instead (that's
 * ReplicaManager, exercised by the real multi-process acceptance run per
 * §8.9's own "cannot be meaningfully tested in-process" note).
 */
class PartitionReplicaStateTest {

    private static final long LAG_MAX_MS = 1000;
    private static final TopicPartition TP = new TopicPartition("orders", 0);

    @Test
    void singleReplicaNoFollowersYetIsTriviallyItsOwnIsrAndHwm() {
        PartitionReplicaState state = new PartitionReplicaState(TP, List.of(0), 0, 0);
        assertEquals(List.of(0), state.isr(42, LAG_MAX_MS, 0));
        assertEquals(42, state.highWaterMark(42, LAG_MAX_MS, 0));
    }

    @Test
    void followerExactlyAtTheLagBoundaryIsStillInTheIsr() {
        PartitionReplicaState state = new PartitionReplicaState(TP, List.of(0, 1), 0, 0);
        state.recordFollowerFetch(1, 10, 10, 0); // caught up to our LEO (10) at time 0
        // now - lastCaughtUpMillis == LAG_MAX_MS exactly: PRD says "within
        // replica.lag.time.max.ms", and the boundary itself must count as
        // still within it, not just outside it.
        assertTrue(state.isr(10, LAG_MAX_MS, LAG_MAX_MS).contains(1));
    }

    @Test
    void followerOneMillisecondPastTheLagBoundaryDropsOutOfTheIsr() {
        PartitionReplicaState state = new PartitionReplicaState(TP, List.of(0, 1), 0, 0);
        state.recordFollowerFetch(1, 10, 10, 0);
        assertFalse(state.isr(10, LAG_MAX_MS, LAG_MAX_MS + 1).contains(1));
    }

    @Test
    void chronicallyLaggingFollowerIsExcludedEvenThoughItKeepsFetching() {
        // The non-obvious case this project's own javadoc calls out: a
        // follower that never actually catches up (production keeps
        // outpacing it) must age out of the ISR exactly like a crashed
        // one would, even though it's still issuing fetches on schedule.
        PartitionReplicaState state = new PartitionReplicaState(TP, List.of(0, 1), 0, 0);
        state.recordFollowerFetch(1, 5, 20, 0);              // behind (LEO is 20) at t=0 — never marked caught up
        state.recordFollowerFetch(1, 8, 25, LAG_MAX_MS + 1); // still behind, still polling, well past the lag window
        assertFalse(state.isr(25, LAG_MAX_MS, LAG_MAX_MS + 1).contains(1));
    }

    @Test
    void highWaterMarkIsTheMinimumFetchedOffsetAcrossTheCurrentIsr() {
        PartitionReplicaState state = new PartitionReplicaState(TP, List.of(0, 1, 2), 0, 0);
        state.recordFollowerFetch(1, 30, 30, 0); // caught up, in ISR
        state.recordFollowerFetch(2, 10, 30, 0); // behind, but recently caught up before falling behind again is not modeled here — just behind, still "recently seen" so still in ISR window
        long hwm = state.highWaterMark(30, LAG_MAX_MS, 0);
        assertEquals(10, hwm); // the slowest IN-ISR replica caps it, even though the leader itself is at 30
    }

    @Test
    void higherEpochIsAlwaysAdopted() {
        PartitionReplicaState state = new PartitionReplicaState(TP, List.of(0, 1), 0, 0);
        assertTrue(state.reconcile(5, 1, 100, 0));
        assertEquals(5, state.epoch());
        assertEquals(1, state.believedLeaderId());
        assertEquals(100, state.epochStartOffset());
    }

    @Test
    void lowerOrEqualEpochWithAHigherLeaderIdIsRejected() {
        PartitionReplicaState state = new PartitionReplicaState(TP, List.of(0, 1), 0, 0);
        state.promoteSelf(0, 0); // epoch=1, leader=0
        assertFalse(state.reconcile(1, 1, 0, 0)); // same epoch, but their leaderId (1) is not lower than ours (0)
        assertFalse(state.reconcile(0, 1, 0, 0)); // lower epoch entirely
        assertEquals(0, state.believedLeaderId());
    }

    @Test
    void epochZeroDisagreementIsNeverTieBroken() {
        // Found via real multi-process testing (see JOURNAL.md,
        // 2026-09-12): epoch 0 is a static, independently-computed
        // starting belief, not the result of a real promotion. A broker
        // with a wrong/incomplete cluster view (e.g. a misconfigured
        // broker.peers) reporting itself as leader at epoch 0 for a
        // partition it isn't even really a replica of must NOT be able to
        // override a correct, healthy belief just by having a lower id.
        // assignedReplicas=[2,0,1] means broker 2's OWN default belief is
        // "I lead this partition" (first in the list) — exactly what a
        // correctly-configured cluster would also compute for broker 2.
        PartitionReplicaState state = new PartitionReplicaState(TP, List.of(2, 0, 1), 2, 0);
        // Someone else claims leader=1 at the SAME epoch 0 — under the old,
        // unconditional tie-break this would have won (1 < 2) and silently
        // overwritten a correct belief with a wrong one.
        assertFalse(state.reconcile(0, 1, 0, 0));
        assertEquals(2, state.believedLeaderId()); // unchanged: epoch-0 disagreements are never adopted
    }

    @Test
    void sameEpochTieIsBrokenByLowerBrokerId() {
        // PRD §8.1's named split-brain weakness: two brokers can both
        // promote themselves independently during a partition, landing on
        // the SAME epoch value. Every broker must still converge on one
        // answer once they're back in contact — lower broker id wins.
        PartitionReplicaState state = new PartitionReplicaState(TP, List.of(0, 1, 2), 2, 0);
        state.promoteSelf(0, 0); // broker 2 promotes itself: epoch=1, leader=2
        assertTrue(state.reconcile(1, 0, 0, 0)); // broker 0 ALSO promoted at epoch 1 — lower id wins the tie
        assertEquals(0, state.believedLeaderId());
    }

    @Test
    void promotingSelfIncrementsEpochAndClearsStaleFollowerBookkeeping() {
        PartitionReplicaState state = new PartitionReplicaState(TP, List.of(0, 1), 1, 0);
        state.recordFollowerFetch(0, 5, 5, 0);
        state.promoteSelf(100, 5);
        assertEquals(1, state.epoch());
        assertEquals(5, state.epochStartOffset());
        assertTrue(state.isLeader());
        // A fresh term starts with nobody yet proven caught up under the
        // new leader — stale state from before promotion must not let a
        // follower look already in-sync when it hasn't fetched even once
        // from us as the new leader.
        assertEquals(List.of(1), state.isr(5, LAG_MAX_MS, 100));
    }
}
