package com.advaith.broker.api;

import com.advaith.broker.log.LogManager;
import com.advaith.broker.log.PartitionLog;
import com.advaith.broker.network.SelectorTicker;
import com.advaith.broker.protocol.ApiKey;
import com.advaith.broker.protocol.Errors;
import com.advaith.broker.protocol.ProtocolReader;
import com.advaith.broker.protocol.ProtocolWriter;
import com.advaith.broker.replication.ReplicaManager;
import com.advaith.broker.replication.ReplicationConfig;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * Reads record batches back from an offset (PRD §5.3), and — since M3
 * (PRD §7.3) — actually long-polls instead of always answering
 * immediately. Design: when a Fetch has nothing (or not enough, per
 * {@code min_bytes}) to return and the client is willing to wait
 * ({@code max_wait_ms > 0}), the request is "parked" in {@code pending}
 * rather than answered, and completed later from one of two triggers:
 * {@link #onAppended} (a matching Produce landed) or {@link #tick} (the
 * deadline passed with nothing new). Both are driven from the same single
 * selector thread that calls {@link #handle} — this class holds no lock
 * because nothing else ever touches {@code pending} concurrently, exactly
 * the same single-threaded invariant M1's handlers already relied on.
 *
 * <p>Since M4 (PRD §8.2/§8.3/§8.4), {@code replica_id} finally means
 * something: a value other than -1 marks this as inter-broker replication
 * traffic (a peer's PeerReplicator, not a real consumer), and three things
 * change for it alone: (1) it's exempt from the high-water-mark cap
 * below — a replica must be able to read all the way to the raw log end,
 * past what's merely confirmed on every in-sync replica, or it could
 * never BECOME in-sync; (2) the {@code high_watermark} response field is
 * repurposed to carry this broker's raw {@code logEndOffset()} instead —
 * exactly the number a replica needs to detect it has diverged (PRD
 * §8.3); (3) three extra fields (epoch, believed leader, and that
 * leader's own epoch-start offset — see PartitionReplicaState's javadoc
 * for why the third one is load-bearing, not decorative) are read per
 * partition on the request and written per partition on the response —
 * a broker-only epoch/leader gossip extension (see ReplicaManager/
 * PeerReplicator's javadocs) that real Fetch v4 has no room for and that
 * a real client, which never sets replica_id, never sees.
 */
public final class FetchHandler implements ApiHandler, SelectorTicker, FetchCompletionListener {

    /** Default server-side ceiling on a client's requested max_wait_ms (PRD §7.6) — a client may ask for less, never more. */
    public static final int DEFAULT_MAX_WAIT_MS_CAP = 500;

    private final LogManager logManager;
    private final int maxWaitMsCap;
    private final ReplicaManager replicaManager;

    // Single selector thread only: handle(), onAppended(), and tick() are
    // never called concurrently with each other, so a plain ArrayList is
    // correct without synchronization.
    private final List<PendingFetch> pending = new ArrayList<>();

    public FetchHandler(LogManager logManager) {
        this(logManager, DEFAULT_MAX_WAIT_MS_CAP);
    }

    public FetchHandler(LogManager logManager, int maxWaitMsCap) {
        // Single-broker default: every partition trivially self-led (see
        // ReplicaManager's javadoc) — matches this class's own pre-M4
        // behavior exactly for every caller that doesn't pass one in.
        this(logManager, maxWaitMsCap, new ReplicaManager(0, logManager, ReplicationConfig.singleBrokerDefault()));
    }

    public FetchHandler(LogManager logManager, int maxWaitMsCap, ReplicaManager replicaManager) {
        this.logManager = logManager;
        this.maxWaitMsCap = maxWaitMsCap;
        this.replicaManager = replicaManager;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.FETCH;
    }

    @Override
    public byte[] handle(RequestContext context, ProtocolReader request) {
        int replicaId = request.readInt32(); // -1 from a normal consumer; since M4, a peer broker's own broker.id otherwise
        boolean isReplicaFetch = replicaId != -1;
        int maxWaitMs = Math.min(request.readInt32(), maxWaitMsCap);
        int minBytes = request.readInt32();
        request.readInt32(); // max_bytes — the overall request-level cap; we only enforce the per-partition cap below
        request.readInt8();  // isolation_level — 0 = read_uncommitted is the only mode; transactions are a non-goal

        List<TopicRequest> topics = request.readArray(r -> readTopicRequest(r, isReplicaFetch));

        FetchComputation computation = compute(topics, replicaId, isReplicaFetch);
        if (isSatisfied(computation, minBytes) || maxWaitMs <= 0) {
            return writeResponse(computation.results(), isReplicaFetch);
        }

        // Nothing satisfies min_bytes yet and the client asked to wait —
        // answering right now would just teach a real consumer to busy-poll
        // us (PRD §7.3's whole reason for existing). Park it: remember
        // enough (the parsed request, not just the socket) to redo this
        // exact computation later, either from a matching Produce or a
        // timer sweep, and return null so RequestDispatcher sends nothing
        // now — this handler will send the eventual response itself.
        long deadlineMillis = System.currentTimeMillis() + maxWaitMs;
        pending.add(new PendingFetch(topics, minBytes, deadlineMillis, context, replicaId, isReplicaFetch));
        return null;
    }

    /**
     * Called by ProduceHandler right after it appends a batch. Only
     * pending fetches that actually named this exact partition are worth
     * re-checking — everything else couldn't possibly have changed.
     */
    @Override
    public void onAppended(String topic, int partitionIndex) {
        Iterator<PendingFetch> it = pending.iterator();
        while (it.hasNext()) {
            PendingFetch p = it.next();
            if (!p.watches(topic, partitionIndex)) {
                continue;
            }
            FetchComputation computation = compute(p.topics(), p.replicaId(), p.isReplicaFetch());
            if (isSatisfied(computation, p.minBytes())) {
                it.remove();
                p.context().sendAsync(writeResponse(computation.results(), p.isReplicaFetch()));
            }
        }
    }

    /** SelectorTicker: how long NetworkServer's select() may block before the soonest pending fetch needs to time out. */
    @Override
    public long millisUntilNextDeadline() {
        if (pending.isEmpty()) {
            return -1;
        }
        long soonest = Long.MAX_VALUE;
        for (PendingFetch p : pending) {
            soonest = Math.min(soonest, p.deadlineMillis());
        }
        return Math.max(0, soonest - System.currentTimeMillis());
    }

    /** SelectorTicker: complete anything whose deadline has passed — with whatever's available, never an error (PRD §7.3/real Kafka). */
    @Override
    public void tick() {
        long now = System.currentTimeMillis();
        Iterator<PendingFetch> it = pending.iterator();
        while (it.hasNext()) {
            PendingFetch p = it.next();
            if (p.deadlineMillis() <= now) {
                it.remove();
                FetchComputation computation = compute(p.topics(), p.replicaId(), p.isReplicaFetch());
                p.context().sendAsync(writeResponse(computation.results(), p.isReplicaFetch()));
            }
        }
    }

    /**
     * The one true "is there enough here to answer" predicate, used by the
     * immediate path, the onAppended retry, and — implicitly, since it's
     * the same computation — the timeout sweep's final read. An error on
     * any partition always counts as satisfied: waiting cannot fix
     * "unknown topic" or "offset out of range", so there's nothing to gain
     * by parking on one.
     */
    private static boolean isSatisfied(FetchComputation computation, int minBytes) {
        return computation.hasError() || computation.totalBytes() >= minBytes;
    }

    // ---- shared read logic: used for the immediate path, the onAppended retry, and the timeout sweep alike ----

    private record PartitionRequest(int index, long fetchOffset, int maxBytes, int senderEpoch, int senderBelievedLeader, long senderEpochStartOffset) {}
    private record TopicRequest(String name, List<PartitionRequest> partitions) {}
    private record PartitionResult(int index, short errorCode, long highWatermarkField, byte[] records, int responderEpoch, int responderBelievedLeader, long responderEpochStartOffset) {}
    private record TopicResult(String name, List<PartitionResult> partitions) {}
    private record FetchComputation(List<TopicResult> results, long totalBytes, boolean hasError) {}

    private record PendingFetch(List<TopicRequest> topics, int minBytes, long deadlineMillis, RequestContext context,
                                 int replicaId, boolean isReplicaFetch) {
        boolean watches(String topic, int partitionIndex) {
            for (TopicRequest t : topics) {
                if (!t.name().equals(topic)) {
                    continue;
                }
                for (PartitionRequest p : t.partitions()) {
                    if (p.index() == partitionIndex) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    private FetchComputation compute(List<TopicRequest> topics, int replicaId, boolean isReplicaFetch) {
        List<TopicResult> results = new ArrayList<>(topics.size());
        long totalBytes = 0;
        boolean hasError = false;
        for (TopicRequest topic : topics) {
            List<PartitionResult> partitionResults = new ArrayList<>(topic.partitions().size());
            for (PartitionRequest p : topic.partitions()) {
                Optional<PartitionLog> partitionLog = logManager.getPartition(topic.name(), p.index());
                if (partitionLog.isEmpty()) {
                    partitionResults.add(new PartitionResult(p.index(), Errors.UNKNOWN_TOPIC_OR_PARTITION, -1, null, 0, 0, 0));
                    hasError = true;
                    continue;
                }
                PartitionLog log = partitionLog.get();

                if (isReplicaFetch) {
                    // PRD §8.3: update follower bookkeeping (and reconcile
                    // epoch — see ReplicaManager's javadoc) BEFORE reading,
                    // so this same response already reflects anything just
                    // learned.
                    replicaManager.onIncomingReplicaFetch(topic.name(), p.index(), replicaId, p.fetchOffset(),
                            p.senderEpoch(), p.senderBelievedLeader(), p.senderEpochStartOffset());
                } else if (!replicaManager.isLeaderFor(topic.name(), p.index())) {
                    // PRD §8.5: a normal consumer talking to a broker that
                    // isn't (or no longer is) this partition's leader must
                    // be told so plainly, not served a stale/incomplete
                    // view — its fix is to refresh Metadata and reconnect
                    // to whoever the real leader is now.
                    ReplicaManager.EpochInfo info = replicaManager.epochInfo(topic.name(), p.index());
                    partitionResults.add(new PartitionResult(p.index(), Errors.NOT_LEADER_OR_FOLLOWER, -1, null,
                            info.epoch(), info.believedLeaderId(), info.epochStartOffset()));
                    hasError = true;
                    continue;
                }

                try {
                    long ceiling = isReplicaFetch ? Long.MAX_VALUE : replicaManager.highWaterMark(topic.name(), p.index());
                    byte[] records = log.read(p.fetchOffset(), p.maxBytes(), ceiling);
                    totalBytes += records.length;
                    // For a replica fetch, PRD §8.3 repurposes this field
                    // to carry our raw log end (what a follower needs to
                    // detect divergence) instead of the ISR-capped HWM a
                    // normal consumer must never read past.
                    long highWatermarkField = isReplicaFetch ? log.logEndOffset() : ceiling;
                    ReplicaManager.EpochInfo info = replicaManager.epochInfo(topic.name(), p.index());
                    partitionResults.add(new PartitionResult(p.index(), Errors.NONE, highWatermarkField, records,
                            info.epoch(), info.believedLeaderId(), info.epochStartOffset()));
                } catch (PartitionLog.OffsetOutOfRangeException e) {
                    ReplicaManager.EpochInfo info = replicaManager.epochInfo(topic.name(), p.index());
                    partitionResults.add(new PartitionResult(p.index(), Errors.OFFSET_OUT_OF_RANGE, log.logEndOffset(), null,
                            info.epoch(), info.believedLeaderId(), info.epochStartOffset()));
                    hasError = true;
                }
            }
            results.add(new TopicResult(topic.name(), partitionResults));
        }
        return new FetchComputation(results, totalBytes, hasError);
    }

    private TopicRequest readTopicRequest(ProtocolReader reader, boolean isReplicaFetch) {
        String topicName = reader.readString();
        List<PartitionRequest> partitions = reader.readArray(r -> readPartitionRequest(r, isReplicaFetch));
        return new TopicRequest(topicName, partitions);
    }

    private static PartitionRequest readPartitionRequest(ProtocolReader reader, boolean isReplicaFetch) {
        int partitionIndex = reader.readInt32();
        long fetchOffset = reader.readInt64();
        int partitionMaxBytes = reader.readInt32();
        // M4 broker-only extension (see class javadoc) — absent from a
        // real client's request, which never sets replica_id.
        int senderEpoch = isReplicaFetch ? reader.readInt32() : 0;
        int senderBelievedLeader = isReplicaFetch ? reader.readInt32() : 0;
        long senderEpochStartOffset = isReplicaFetch ? reader.readInt64() : 0;
        return new PartitionRequest(partitionIndex, fetchOffset, partitionMaxBytes, senderEpoch, senderBelievedLeader, senderEpochStartOffset);
    }

    private static byte[] writeResponse(List<TopicResult> results, boolean isReplicaFetch) {
        ProtocolWriter response = new ProtocolWriter();
        response.writeInt32(0); // throttle_time_ms
        response.writeArray(results, (w, result) -> writeTopicResult(w, result, isReplicaFetch));
        return response.toByteArray();
    }

    private static void writeTopicResult(ProtocolWriter w, TopicResult result, boolean isReplicaFetch) {
        w.writeString(result.name());
        w.writeArray(result.partitions(), (pw, partition) -> {
            pw.writeInt32(partition.index());
            pw.writeInt16(partition.errorCode());
            pw.writeInt64(partition.highWatermarkField());
            // last_stable_offset only differs from high_watermark once transactions
            // can leave uncommitted records in between the two — a non-goal here
            // (PRD §2), so we report the same value for both.
            pw.writeInt64(partition.highWatermarkField());
            pw.writeArray(null, (aw, ignored) -> {}); // aborted_transactions — PRD §5.3: "send null"
            pw.writeNullableBytes(partition.records());
            if (isReplicaFetch) {
                // M4 broker-only extension (see class javadoc) — a real
                // client never reads these because it never triggers them.
                pw.writeInt32(partition.responderEpoch());
                pw.writeInt32(partition.responderBelievedLeader());
                pw.writeInt64(partition.responderEpochStartOffset());
            }
        });
    }
}
