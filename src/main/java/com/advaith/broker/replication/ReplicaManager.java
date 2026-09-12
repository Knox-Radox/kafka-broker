package com.advaith.broker.replication;

import com.advaith.broker.log.LogManager;
import com.advaith.broker.log.PartitionLog;
import com.advaith.broker.protocol.Errors;
import com.advaith.broker.record.RecordBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The M4 (PRD §8) "brain" every replication-aware handler and every
 * PeerReplicator thread talks to: which broker leads which partition (as
 * far as THIS broker currently believes — there is no shared source of
 * truth, per §8.1's lease-not-quorum decision), the ISR, the high-water
 * mark, and epoch fencing. One instance for the whole broker, one
 * {@link PartitionReplicaState} per partition (see that class for the
 * concurrency story — this is the one part of the project touched from
 * more than the single selector thread).
 *
 * Single-broker mode (no configured peers) is not a special case: every
 * partition's assigned-replica list is just {@code [selfBrokerId]}, so
 * every query below answers exactly as it would have pre-M4 (leader is
 * always self, ISR is always just self, HWM is always the raw log end) —
 * the same "the general mechanism's degenerate case needs no branching"
 * pattern as SelectorTicker.NONE and FetchCompletionListener.NONE.
 */
public final class ReplicaManager {

    private static final Logger log = LoggerFactory.getLogger(ReplicaManager.class);

    /** See {@link #checkForPromotions}'s javadoc for why this is a multiple of the lease timeout, not the timeout itself. */
    private static final int STARTUP_GRACE_MULTIPLE = 3;

    private final int selfBrokerId;
    private final LogManager logManager;
    private final ReplicationConfig config;
    private final Map<TopicPartition, PartitionReplicaState> states = new ConcurrentHashMap<>();
    private final List<PeerReplicator> peerReplicators = new ArrayList<>();
    private volatile HwmAdvancedListener hwmAdvancedListener = HwmAdvancedListener.NONE;
    /**
     * When this broker's OWN replication machinery came up (see
     * JOURNAL.md, 2026-09-12) — every {@link PartitionReplicaState}'s
     * lease clock starts ticking the instant it's constructed, but a
     * peer this broker needs to hear from may simply not have finished
     * booting yet (brokers in a real deployment don't all reach "ready"
     * at the exact same instant). Without a startup grace period, that
     * ordinary staggered-startup delay looks identical to "the leader
     * died", and a broker can promote itself over a leader that was never
     * actually down. {@link #checkForPromotions} refuses to promote
     * anything until this long after {@link #start()} was called.
     */
    private volatile long startedAtMillis = Long.MAX_VALUE;

    public ReplicaManager(int selfBrokerId, LogManager logManager, ReplicationConfig config) {
        this.selfBrokerId = selfBrokerId;
        this.logManager = logManager;
        this.config = config;

        TreeSet<Integer> allBrokerIds = new TreeSet<>();
        allBrokerIds.add(selfBrokerId);
        for (PeerInfo peer : config.peers()) {
            allBrokerIds.add(peer.brokerId());
        }
        List<Integer> sortedBrokerIds = List.copyOf(allBrokerIds);

        long now = System.currentTimeMillis();
        for (String topic : logManager.topicNames()) {
            int partitionCount = logManager.partitionCount(topic);
            for (int partition = 0; partition < partitionCount; partition++) {
                List<Integer> replicas = ReplicaAssignment.replicasFor(topic, partition, sortedBrokerIds, config.replicationFactor());
                states.put(new TopicPartition(topic, partition), new PartitionReplicaState(
                        new TopicPartition(topic, partition), replicas, selfBrokerId, now));
            }
        }
    }

    public void setHwmAdvancedListener(HwmAdvancedListener listener) {
        this.hwmAdvancedListener = listener;
    }

    /** Starts one background replicator thread per configured peer (PRD §8.2) — a no-op in single-broker mode. */
    public void start() {
        startedAtMillis = System.currentTimeMillis();
        for (PeerInfo peer : config.peers()) {
            PeerReplicator replicator = new PeerReplicator(selfBrokerId, peer, this, config);
            peerReplicators.add(replicator);
            replicator.start();
        }
    }

    public void stop() {
        for (PeerReplicator replicator : peerReplicators) {
            replicator.shutdown();
        }
    }

    // ==================== queries used by client-facing handlers ====================

    public boolean isLeaderFor(String topic, int partition) {
        PartitionReplicaState state = states.get(new TopicPartition(topic, partition));
        if (state == null) {
            return true; // an unassigned/unknown partition falls back to pre-M4 behavior rather than mysteriously rejecting everything
        }
        state.lock.lock();
        try {
            return state.isLeader();
        } finally {
            state.lock.unlock();
        }
    }

    public int leaderIdFor(String topic, int partition) {
        PartitionReplicaState state = states.get(new TopicPartition(topic, partition));
        if (state == null) {
            return selfBrokerId;
        }
        state.lock.lock();
        try {
            return state.believedLeaderId();
        } finally {
            state.lock.unlock();
        }
    }

    public List<Integer> replicasFor(String topic, int partition) {
        PartitionReplicaState state = states.get(new TopicPartition(topic, partition));
        return state == null ? List.of(selfBrokerId) : state.assignedReplicas; // immutable after construction: safe to hand out directly, no lock needed
    }

    public List<Integer> isrFor(String topic, int partition) {
        PartitionReplicaState state = states.get(new TopicPartition(topic, partition));
        if (state == null) {
            return List.of(selfBrokerId);
        }
        PartitionLog log = requirePartition(topic, partition);
        state.lock.lock();
        try {
            return state.isr(log.logEndOffset(), config.replicaLagTimeMaxMs(), System.currentTimeMillis());
        } finally {
            state.lock.unlock();
        }
    }

    /** PRD §8.4: the offset ceiling a normal consumer's Fetch must respect. Falls back to the raw log end when replication isn't tracked for this partition (or is trivially just self). */
    public long highWaterMark(String topic, int partition) {
        PartitionReplicaState state = states.get(new TopicPartition(topic, partition));
        PartitionLog log = requirePartition(topic, partition);
        if (state == null) {
            return log.logEndOffset();
        }
        state.lock.lock();
        try {
            return state.highWaterMark(log.logEndOffset(), config.replicaLagTimeMaxMs(), System.currentTimeMillis());
        } finally {
            state.lock.unlock();
        }
    }

    /** Called by ProduceHandler right before appending, so the batch's on-disk partitionLeaderEpoch field (PRD §8.5) reflects the epoch it was actually accepted under. */
    public int currentEpoch(String topic, int partition) {
        PartitionReplicaState state = states.get(new TopicPartition(topic, partition));
        if (state == null) {
            return 0;
        }
        state.lock.lock();
        try {
            return state.epoch();
        } finally {
            state.lock.unlock();
        }
    }

    /** Called by ProduceHandler after a successful leader-side append — the HWM may now be reachable for a parked acks=-1 request (trivially, in the single-replica case). */
    public void afterLeaderAppend(String topic, int partition) {
        fireHwmAdvanced(topic, partition);
    }

    // ==================== the server side of a replica Fetch (PRD §8.3/§8.5) ====================

    /**
     * Called by FetchHandler for every incoming replica_id != -1 Fetch,
     * BEFORE building the response — both halves of that ordering matter:
     * reconciling epoch first means a just-discovered higher epoch is
     * already reflected in what this same response reports back (PRD
     * §8.3's "updates ... before building the response"), and recording
     * the follower's position is what next lets isr()/highWaterMark() see
     * it.
     */
    public void onIncomingReplicaFetch(String topic, int partition, int followerBrokerId, long fetchOffset,
                                        int senderEpoch, int senderBelievedLeader, long senderEpochStartOffset) {
        TopicPartition tp = new TopicPartition(topic, partition);
        PartitionReplicaState state = states.get(tp);
        if (state == null) {
            return;
        }
        PartitionLog log = requirePartition(topic, partition);
        long now = System.currentTimeMillis();
        state.lock.lock();
        try {
            boolean adopted = state.reconcile(senderEpoch, senderBelievedLeader, senderEpochStartOffset, now);
            truncateToEpochStartIfAdopted(tp, state, log, adopted);
            state.recordFollowerFetch(followerBrokerId, fetchOffset, log.logEndOffset(), now);
        } finally {
            state.lock.unlock();
        }
        fireHwmAdvanced(topic, partition);
    }

    public record EpochInfo(int epoch, int believedLeaderId, long epochStartOffset) {}

    /** What FetchHandler embeds back in a replica-fetch response's extension fields (PRD §8.5's bidirectional gossip — see PeerReplicator's javadoc). */
    public EpochInfo epochInfo(String topic, int partition) {
        PartitionReplicaState state = states.get(new TopicPartition(topic, partition));
        if (state == null) {
            return new EpochInfo(0, selfBrokerId, 0);
        }
        state.lock.lock();
        try {
            return new EpochInfo(state.epoch(), state.believedLeaderId(), state.epochStartOffset());
        } finally {
            state.lock.unlock();
        }
    }

    /**
     * Shared post-reconcile step for both the server side (above) and the
     * client side ({@link #applyReplicaFetchResult}) — PRD §8.3's
     * truncation-on-divergence, triggered the instant a HIGHER epoch is
     * adopted from EITHER direction of the gossip exchange (see
     * {@link PartitionReplicaState#reconcile}'s javadoc for why
     * {@code epochStartOffset}, not each batch's own stamped epoch, is
     * the correct truncation boundary). A no-op for a healthy replica
     * that was never actually ahead of that boundary — this only ever
     * discards data, never invents a shortfall. Caller must hold
     * {@code state.lock}. Returns whether a truncation actually happened
     * — see JOURNAL.md (2026-09-12) for why the caller MUST discard
     * whatever records it already fetched this same round when it did.
     */
    private boolean truncateToEpochStartIfAdopted(TopicPartition tp, PartitionReplicaState state, PartitionLog partitionLog, boolean epochAdopted) {
        if (!epochAdopted) {
            return false;
        }
        long epochStart = state.epochStartOffset();
        if (partitionLog.logEndOffset() > epochStart) {
            log.warn("partition {}: adopted epoch {} (leader {}, started at offset {}) — truncating our log end {} back to it",
                    tp, state.epoch(), state.believedLeaderId(), epochStart, partitionLog.logEndOffset());
            partitionLog.truncateTo(epochStart);
            return true;
        }
        return false;
    }

    private void fireHwmAdvanced(String topic, int partition) {
        hwmAdvancedListener.onHighWaterMarkAdvanced(topic, partition, highWaterMark(topic, partition));
    }

    // ==================== used by PeerReplicator (package-private surface) ====================

    List<TopicPartition> partitionsSharedWith(int peerBrokerId) {
        List<TopicPartition> shared = new ArrayList<>();
        for (Map.Entry<TopicPartition, PartitionReplicaState> entry : states.entrySet()) {
            List<Integer> replicas = entry.getValue().assignedReplicas;
            if (replicas.contains(selfBrokerId) && replicas.contains(peerBrokerId)) {
                shared.add(entry.getKey());
            }
        }
        return shared;
    }

    record OutboundSnapshot(long fetchOffset, int epoch, int believedLeaderId, long epochStartOffset) {}

    /** What PeerReplicator sends as this partition's request-side extension fields — our own current belief, at request-build time. */
    OutboundSnapshot snapshotForRequest(TopicPartition tp) {
        PartitionReplicaState state = states.get(tp);
        PartitionLog log = requirePartition(tp.topic(), tp.partition());
        state.lock.lock();
        try {
            return new OutboundSnapshot(log.logEndOffset(), state.epoch(), state.believedLeaderId(), state.epochStartOffset());
        } finally {
            state.lock.unlock();
        }
    }

    /**
     * Processes one partition's result from a Fetch WE issued as a
     * replica (PeerReplicator's response handling). Reconciles epoch
     * first (so a fresher belief from this peer, and its truncation
     * boundary if adopted, are both in effect before anything below
     * runs), then — only while we still believe this exact peer is the
     * leader after that reconciliation — appends whatever new data came
     * back.
     *
     * <p><b>Optimistic offset check before appending</b> (see JOURNAL.md,
     * 2026-09-12): {@code recordsBytes} in THIS response were fetched
     * starting at {@code requestedFetchOffset} — read from our log BEFORE
     * this round's request went out. If our log's end no longer equals
     * that value by the time the response comes back, our tail moved in
     * the meantime (a truncation this SAME round's own reconciliation
     * just triggered, or — the rarer, still real case this specifically
     * guards against — one triggered by a DIFFERENT peer's round that
     * raced ahead of this one), and appending anyway would stamp data
     * fetched from the WRONG position onto whatever offset our log
     * currently ends at, silently misaligning every record from there on
     * — the same failure family as the divergence this whole mechanism
     * exists to prevent, just self-inflicted instead of caused by a
     * partition. Discarding this round's data and letting the NEXT round
     * re-request from wherever our log actually ends now is what's safe.
     */
    void applyReplicaFetchResult(TopicPartition tp, int peerBrokerId, short errorCode,
                                  int responderEpoch, int responderBelievedLeader, long responderEpochStartOffset,
                                  long requestedFetchOffset, byte[] recordsBytes) {
        PartitionReplicaState state = states.get(tp);
        if (state == null) {
            return;
        }
        PartitionLog partitionLog = requirePartition(tp.topic(), tp.partition());
        long now = System.currentTimeMillis();

        state.lock.lock();
        try {
            boolean adopted = state.reconcile(responderEpoch, responderBelievedLeader, responderEpochStartOffset, now);
            truncateToEpochStartIfAdopted(tp, state, partitionLog, adopted);

            boolean stillBelievePeerIsLeader = state.believedLeaderId() == peerBrokerId;
            if (!stillBelievePeerIsLeader || errorCode != Errors.NONE) {
                return; // nothing about this peer's data is authoritative to us right now
            }
            state.recordLeaderContact(now);

            if (partitionLog.logEndOffset() != requestedFetchOffset) {
                log.debug("partition {}: log end moved ({} -> {}) since this round's Fetch was sent — discarding its records, next round will re-request from the right place",
                        tp, requestedFetchOffset, partitionLog.logEndOffset());
                return;
            }

            if (recordsBytes != null && recordsBytes.length > 0) {
                appendReplicatedBytes(partitionLog, recordsBytes);
            }
        } finally {
            state.lock.unlock();
        }
    }

    /**
     * Splits a fetched (possibly multi-batch) byte blob back into
     * individual batches and appends each one through the exact same
     * PartitionLog.append() a Produce uses (PRD §8.3: "a follower's local
     * log is built by literally replaying the leader's batches through
     * the same storage engine, not a separate mechanism"). Re-running
     * RecordBatch.parse() per batch here (rather than trusting the bytes
     * blindly) is deliberate even though the leader already validated
     * them once at produce time — this is freshly-received network input,
     * and re-checking its CRC is cheap insurance against corruption
     * introduced between brokers, unlike the "don't re-checksum on every
     * client read" call already made for already-verified local disk data
     * (see RecordBatch.peekLocation()'s javadoc).
     */
    private static void appendReplicatedBytes(PartitionLog partitionLog, byte[] multiBatchBytes) {
        int position = 0;
        while (position < multiBatchBytes.length) {
            RecordBatch.BatchLocation location = RecordBatch.peekLocation(multiBatchBytes, position);
            byte[] batchBytes = java.util.Arrays.copyOfRange(multiBatchBytes, position, position + location.totalSizeInBytes());
            RecordBatch parsed = RecordBatch.parse(batchBytes); // validates CRC/magic; throws on corruption rather than silently accepting bad replicated data
            partitionLog.append(batchBytes, parsed.recordCount());
            position += location.totalSizeInBytes();
        }
    }

    // ==================== leader-election check (PRD §8.5), driven by PeerReplicator's loop ====================

    /**
     * Called after every round-trip a PeerReplicator completes (success or
     * failure) — cheap enough to run that often since it's just a map
     * scan. For every partition we're a replica of but don't believe we
     * lead, if the believed leader's lease has expired AND we are the
     * lowest-id replica we can currently still reach, promote ourselves.
     */
    void checkForPromotions() {
        long now = System.currentTimeMillis();
        if (now - startedAtMillis < STARTUP_GRACE_MULTIPLE * config.leaderLeaseTimeoutMs()) {
            // Startup grace period (see JOURNAL.md, 2026-09-12): a peer we
            // haven't heard from yet may simply not have finished booting,
            // not have died. A single lease window turned out too tight in
            // practice — three separate JVMs starting concurrently
            // (competing for the same disk/CPU, each also doing its own
            // log recovery per PRD §6.4 before it even opens a listening
            // socket) can easily take longer than one lease timeout to all
            // become reachable, especially the first time any of them
            // starts cold. A multiple of the lease timeout, paid once at
            // cluster bootstrap, is a small price for not promoting over a
            // peer that was never actually down.
            return;
        }
        for (PartitionReplicaState state : states.values()) {
            if (!state.assignedReplicas.contains(selfBrokerId)) {
                continue;
            }
            state.lock.lock();
            try {
                if (state.isLeader()) {
                    continue;
                }
                if (now - state.lastLeaderContactMillis() <= config.leaderLeaseTimeoutMs()) {
                    continue; // the current leader's lease hasn't expired yet
                }
                if (isLowestReachableReplica(state.assignedReplicas, state.believedLeaderId())) {
                    long ourLogEndOffset = requirePartition(state.topicPartition.topic(), state.topicPartition.partition()).logEndOffset();
                    log.warn("partition {}: leader {} unreachable for over {}ms, promoting self (broker {}) to epoch {} starting at offset {}",
                            state.topicPartition, state.believedLeaderId(), config.leaderLeaseTimeoutMs(), selfBrokerId, state.epoch() + 1, ourLogEndOffset);
                    state.promoteSelf(now, ourLogEndOffset);
                }
            } finally {
                state.lock.unlock();
            }
        }
    }

    /** "Highest priority" per PRD §8.5 = lowest broker.id; "live" = reachable per our own PeerReplicators' recent contact, or self trivially. */
    private boolean isLowestReachableReplica(List<Integer> assignedReplicas, int presumedDeadLeaderId) {
        for (Integer replicaId : assignedReplicas) {
            if (replicaId == presumedDeadLeaderId || replicaId == selfBrokerId) {
                continue;
            }
            if (replicaId < selfBrokerId && isReachable(replicaId)) {
                return false; // someone with higher priority than us is still around
            }
        }
        return true;
    }

    private boolean isReachable(int brokerId) {
        for (PeerReplicator replicator : peerReplicators) {
            if (replicator.peerBrokerId() == brokerId) {
                return replicator.isRecentlyReachable(config.leaderLeaseTimeoutMs());
            }
        }
        return false; // no replicator for this id at all — treat as unreachable rather than guessing
    }

    private PartitionLog requirePartition(String topic, int partition) {
        return logManager.getPartition(topic, partition)
                .orElseThrow(() -> new IllegalStateException("replication is tracking " + topic + "-" + partition + " but LogManager has no such partition"));
    }
}
