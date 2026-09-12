package com.advaith.broker.replication;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Everything M4 tracks about one partition's replication, on one broker.
 * Design decision — the one piece of real concurrency this milestone adds
 * (PRD §8.3's per-peer background threads, see PeerReplicator's javadoc
 * for why a dedicated thread per peer was chosen over reusing the shared
 * selector-thread ticker): unlike every class before M4, this one IS
 * touched from more than one thread — the main selector thread (a client
 * Produce/Fetch checking "am I leader") and every PeerReplicator thread
 * (replaying a peer's data, or reconciling epoch info) all read and write
 * the same partition's belief. One lock per partition is the whole
 * concurrency story: touching anything below, or the underlying
 * PartitionLog for this partition, means holding {@link #lock} first.
 * A per-partition lock (not one global lock) is what lets two unrelated
 * partitions' replication traffic proceed independently.
 */
final class PartitionReplicaState {

    final TopicPartition topicPartition;
    final List<Integer> assignedReplicas;
    final int selfBrokerId;
    final ReentrantLock lock = new ReentrantLock();

    // ---- guarded by lock ----
    private int epoch = 0;
    private int believedLeaderId;
    /**
     * The offset at which the CURRENT epoch's leader began accepting
     * writes — i.e. that leader's own logEndOffset() at the exact moment
     * it promoted itself. Needed for truncation (see {@link #reconcile}'s
     * javadoc for why the per-batch partitionLeaderEpoch stamp alone
     * can't do this job).
     */
    private long epochStartOffset = 0;
    /** Only meaningful entries: replicas OTHER than self that have ever issued us a replica Fetch. */
    private final Map<Integer, FollowerState> followerStates = new HashMap<>();
    /** Last time we (as a non-leader) successfully heard from whoever we currently believe leads this partition. */
    private long lastLeaderContactMillis;

    private static final class FollowerState {
        long lastFetchedOffset = 0;
        long lastFetchTimeMillis = 0;
        /** Last time this follower was actually caught up to our own log end — see inSync() for why this, not lastFetchTimeMillis, is what ISR membership keys off. */
        long lastCaughtUpMillis = 0;
    }

    PartitionReplicaState(TopicPartition topicPartition, List<Integer> assignedReplicas, int selfBrokerId, long now) {
        this.topicPartition = topicPartition;
        this.assignedReplicas = assignedReplicas;
        this.selfBrokerId = selfBrokerId;
        this.believedLeaderId = assignedReplicas.get(0); // PRD §8.1: first in the static assignment is the initial leader, cluster-wide
        this.lastLeaderContactMillis = now; // don't start the lease clock already expired before the first tick ever runs
    }

    // ---- role queries (caller must hold lock) ----

    boolean isLeader() {
        return believedLeaderId == selfBrokerId;
    }

    int believedLeaderId() {
        return believedLeaderId;
    }

    int epoch() {
        return epoch;
    }

    long epochStartOffset() {
        return epochStartOffset;
    }

    long lastLeaderContactMillis() {
        return lastLeaderContactMillis;
    }

    /**
     * Monotonic epoch/leader adoption (PRD §8.5's fencing property):
     * whichever side of an exchange knows about a higher epoch wins;
     * ties (both sides independently promoted at the same epoch during a
     * split — PRD §8.1's named split-brain weakness) are broken by lower
     * broker id, so every broker in the cluster converges on the exact
     * same (epoch, leaderId) pair regardless of which pairwise exchange
     * happens to inform it first. Returns true if this call changed our
     * belief (the caller uses this to know whether it needs to react —
     * e.g. stop treating itself as leader, or start a fresh fetch loop
     * against the newly-adopted leader).
     *
     * <p>The tie-break is deliberately gated to {@code theirEpoch > 0}
     * (see JOURNAL.md, 2026-09-12): epoch 0 is every partition's static,
     * independently-computed starting belief (PRD §8.1 — no controller
     * hands it out), not the outcome of an actual promotion, so two
     * brokers disagreeing about who leads a partition AT epoch 0 can only
     * mean one of them has a wrong/incomplete view of the cluster (a
     * misconfigured broker.peers list, most likely) — not a real
     * simultaneous-promotion split-brain. Applying the "lower id wins"
     * rule there would silently let one broker's bad config corrupt an
     * otherwise-correct belief on a perfectly healthy partition, possibly
     * one it was never even a legitimate replica of in the first place.
     *
     * <p><b>Why this also hands back {@code epochStartOffset}, not just
     * epoch/leaderId</b> (see JOURNAL.md, 2026-09-12): the obvious idea —
     * "when my epoch advances, truncate anything in my own log stamped
     * with the OLD epoch" — does not work. A fenced-off old leader that
     * keeps accepting client writes while partitioned (PRD §8.1's named
     * weakness) never learns to increment its own epoch, precisely
     * because it doesn't know it's been replaced — so its diverged,
     * should-be-discarded writes are stamped with the SAME epoch number
     * as the universally-agreed history before the split. Epoch alone
     * cannot tell "this batch is part of the agreed history" apart from
     * "this batch is a diverged write that happens to share that epoch
     * number". What CAN distinguish them is the new leader's own record
     * of exactly which offset ITS epoch began at: anything at or past
     * that offset in ANY replica's log — including a perfectly healthy
     * follower's, which just means a little wasted re-fetching, not
     * corruption — is superseded the instant a higher epoch is adopted,
     * and gets truncated and rebuilt from the new leader from scratch.
     */
    boolean reconcile(int theirEpoch, int theirBelievedLeaderId, long theirEpochStartOffset, long now) {
        boolean adopt = theirEpoch > epoch || (theirEpoch == epoch && theirEpoch > 0 && theirBelievedLeaderId < believedLeaderId);
        if (!adopt) {
            return false;
        }
        epoch = theirEpoch;
        believedLeaderId = theirBelievedLeaderId;
        epochStartOffset = theirEpochStartOffset;
        if (believedLeaderId != selfBrokerId) {
            lastLeaderContactMillis = now; // whoever just told us this IS (implicitly) live proof of a leader existing right now
        }
        return true;
    }

    /**
     * Self-promotion (PRD §8.5): bump the epoch, declare ourselves leader,
     * reset lease bookkeeping. {@code currentLogEndOffset} — our own LEO
     * at this exact moment — becomes this new epoch's start offset, the
     * boundary every other replica truncates to once it learns of this
     * promotion (see {@link #reconcile}'s javadoc).
     */
    void promoteSelf(long now, long currentLogEndOffset) {
        epoch++;
        believedLeaderId = selfBrokerId;
        epochStartOffset = currentLogEndOffset;
        lastLeaderContactMillis = now;
        followerStates.clear(); // a fresh term starts with nobody yet proven caught up
    }

    /** Called whenever we successfully hear from whoever we currently believe leads this partition (a normal, non-reconciling reply still counts as contact). */
    void recordLeaderContact(long now) {
        lastLeaderContactMillis = now;
    }

    // ---- leader-side bookkeeping (PRD §8.3/§8.4) ----

    /**
     * Updates one follower's tracked position — called on EVERY incoming
     * replica Fetch regardless of our own believed role (PRD §8.3: "every
     * incoming replica Fetch... updates that follower's lastFetchedOffset/
     * lastFetchTime before building the response"). Harmless bookkeeping
     * even when we're not (or no longer) the leader; only consulted by
     * isr()/highWaterMark() when we are.
     */
    void recordFollowerFetch(int followerBrokerId, long fetchOffset, long ourOwnLogEndOffset, long now) {
        FollowerState state = followerStates.computeIfAbsent(followerBrokerId, id -> new FollowerState());
        state.lastFetchedOffset = fetchOffset;
        state.lastFetchTimeMillis = now;
        if (fetchOffset >= ourOwnLogEndOffset) {
            state.lastCaughtUpMillis = now;
        }
    }

    /**
     * PRD §8.4: in the ISR if it hasn't fallen meaningfully behind
     * RECENTLY — not merely "it has fetched at least once". Keying this
     * off lastCaughtUpMillis rather than lastFetchTimeMillis is the
     * deliberate, non-obvious part: a follower that keeps polling but is
     * chronically behind production (never actually catches up) would
     * wrongly stay in the ISR forever under a naive "still fetching"
     * check — lastCaughtUpMillis only advances at the moment it's truly
     * caught up, so a chronic laggard ages out exactly like a crashed one.
     */
    List<Integer> isr(long ourOwnLogEndOffset, long replicaLagTimeMaxMs, long now) {
        List<Integer> isr = new ArrayList<>();
        isr.add(selfBrokerId); // trivially always caught up with itself
        for (Integer replicaId : assignedReplicas) {
            if (replicaId == selfBrokerId) {
                continue;
            }
            FollowerState state = followerStates.get(replicaId);
            if (state == null) {
                continue; // never fetched even once — not in the ISR yet
            }
            if (now - state.lastCaughtUpMillis <= replicaLagTimeMaxMs) {
                isr.add(replicaId);
            }
        }
        return isr;
    }

    /** PRD §8.4: the minimum lastFetchedOffset across the current ISR, including the leader's own (always up to date with itself). */
    long highWaterMark(long ourOwnLogEndOffset, long replicaLagTimeMaxMs, long now) {
        long hwm = ourOwnLogEndOffset;
        for (Integer replicaId : isr(ourOwnLogEndOffset, replicaLagTimeMaxMs, now)) {
            if (replicaId == selfBrokerId) {
                continue;
            }
            FollowerState state = followerStates.get(replicaId);
            hwm = Math.min(hwm, state.lastFetchedOffset);
        }
        return hwm;
    }
}
