package com.advaith.broker.api;

import com.advaith.broker.log.LogManager;
import com.advaith.broker.log.PartitionLog;
import com.advaith.broker.network.SelectorTicker;
import com.advaith.broker.protocol.ApiKey;
import com.advaith.broker.protocol.Errors;
import com.advaith.broker.protocol.ProtocolReader;
import com.advaith.broker.protocol.ProtocolWriter;
import com.advaith.broker.record.RecordBatch;
import com.advaith.broker.replication.HwmAdvancedListener;
import com.advaith.broker.replication.ReplicaManager;
import com.advaith.broker.replication.ReplicationConfig;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Writes record batches to the log (PRD §5.3). Two things in this class are
 * easy to get subtly wrong and both are called out explicitly in the spec:
 * (1) base_offset in the response must be what the log actually assigned,
 * never anything the producer sent, and (2) acks=0 means literally no
 * response at all — not an empty one, none — which this handler expresses
 * by returning null and letting RequestDispatcher's contract handle it.
 *
 * Since M3 (PRD §7.3): every successful append also notifies a
 * {@link FetchCompletionListener} — the other half of long-polling's
 * "wake a parked Fetch the instant qualifying data lands" path, so a
 * consumer doesn't have to wait for the next timer tick to hear about new
 * data on the exact partition it's watching.
 *
 * Since M4 (PRD §8.5/§8.6), {@code acks} finally means something: a
 * non-leader rejects with NOT_LEADER_OR_FOLLOWER before appending at all,
 * and {@code acks=-1} ("all") doesn't respond until the batch's offset is
 * confirmed on every in-sync replica (the high-water mark), using the
 * exact same "park it, resolve it from an event or a tick" mechanism
 * §7.3 built for Fetch long-polling — the third reuse of that one
 * mechanism the PRD calls out by name. This class is therefore now both a
 * {@link SelectorTicker} (times out a produce that never got acked in
 * time) and a {@link HwmAdvancedListener} (wakes one the instant the HWM
 * catches up, the same "don't wait for the next timer tick" reasoning as
 * FetchCompletionListener).
 */
public final class ProduceHandler implements ApiHandler, SelectorTicker, HwmAdvancedListener {

    private final LogManager logManager;
    private final FetchCompletionListener fetchCompletionListener;
    private final ReplicaManager replicaManager;

    // Single selector thread only for THIS list — handle()/tick() run on
    // it, and onHighWaterMarkAdvanced() is also only ever invoked from it
    // (ReplicaManager.afterLeaderAppend/onIncomingReplicaFetch are both
    // called from handler code on the selector thread; the ONE other
    // thread family this milestone introduces, PeerReplicator, never
    // calls into ProduceHandler directly — see ReplicaManager's javadoc).
    private final List<PendingProduce> pending = new ArrayList<>();

    public ProduceHandler(LogManager logManager) {
        this(logManager, FetchCompletionListener.NONE);
    }

    public ProduceHandler(LogManager logManager, FetchCompletionListener fetchCompletionListener) {
        // Single-broker default (see ReplicaManager's javadoc): acks=-1
        // resolves synchronously since the ISR is trivially just this
        // broker — matches this class's pre-M4 behavior for every caller
        // that doesn't pass a real ReplicaManager in.
        this(logManager, fetchCompletionListener, new ReplicaManager(0, logManager, ReplicationConfig.singleBrokerDefault()));
    }

    public ProduceHandler(LogManager logManager, FetchCompletionListener fetchCompletionListener, ReplicaManager replicaManager) {
        this.logManager = logManager;
        this.fetchCompletionListener = fetchCompletionListener;
        this.replicaManager = replicaManager;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.PRODUCE;
    }

    @Override
    public byte[] handle(RequestContext context, ProtocolReader request) {
        String transactionalId = request.readNullableString();
        short acks = request.readInt16();
        int timeoutMs = request.readInt32();

        List<TopicResult> results = request.readArray(r -> readTopicData(r, transactionalId));

        if (acks == 0) {
            return null; // PRD §5.3: acks=0 means the client expects no response at all
        }

        if (acks != -1) {
            // acks=1: respond as soon as the leader has appended locally —
            // unchanged since M1 (PRD §8.6).
            return writeResponse(results);
        }

        List<AckWait> waits = collectAckWaits(results);
        if (waits.isEmpty() || allSatisfied(waits)) {
            // Either every partition already failed synchronously (nothing
            // to wait on), or the ISR is small enough (trivially, just the
            // leader in single-broker mode) that the HWM already covers
            // what we just appended — resolve immediately, same as acks=1.
            return writeResponse(results);
        }

        long deadlineMillis = System.currentTimeMillis() + timeoutMs;
        pending.add(new PendingProduce(results, waits, deadlineMillis, context));
        return null;
    }

    private record AckWait(String topic, int partition, long targetOffsetExclusive) {}

    private record PendingProduce(List<TopicResult> results, List<AckWait> waits, long deadlineMillis, RequestContext context) {
        boolean watches(String topic, int partition) {
            for (AckWait w : waits) {
                if (w.topic().equals(topic) && w.partition() == partition) {
                    return true;
                }
            }
            return false;
        }
    }

    private List<AckWait> collectAckWaits(List<TopicResult> results) {
        List<AckWait> waits = new ArrayList<>();
        for (TopicResult topic : results) {
            for (PartitionResult partition : topic.partitions()) {
                if (partition.errorCode() == Errors.NONE) {
                    waits.add(new AckWait(topic.name(), partition.index(), partition.baseOffset() + partition.recordCount()));
                }
            }
        }
        return waits;
    }

    private boolean allSatisfied(List<AckWait> waits) {
        for (AckWait w : waits) {
            if (replicaManager.highWaterMark(w.topic(), w.partition()) < w.targetOffsetExclusive()) {
                return false;
            }
        }
        return true;
    }

    /** HwmAdvancedListener: wakes any parked acks=-1 produce the instant the partition it's waiting on catches up — don't make it wait for the next timer tick. */
    @Override
    public void onHighWaterMarkAdvanced(String topic, int partition, long highWaterMark) {
        Iterator<PendingProduce> it = pending.iterator();
        while (it.hasNext()) {
            PendingProduce p = it.next();
            if (!p.watches(topic, partition)) {
                continue;
            }
            if (allSatisfied(p.waits())) {
                it.remove();
                p.context().sendAsync(writeResponse(p.results()));
            }
        }
    }

    /** SelectorTicker: how long select() may block before the soonest parked acks=-1 request's own timeout_ms needs to fire. */
    @Override
    public long millisUntilNextDeadline() {
        if (pending.isEmpty()) {
            return -1;
        }
        long soonest = Long.MAX_VALUE;
        for (PendingProduce p : pending) {
            soonest = Math.min(soonest, p.deadlineMillis());
        }
        return Math.max(0, soonest - System.currentTimeMillis());
    }

    /** SelectorTicker: a produce that never got acked by every in-sync replica in time reports NOT_ENOUGH_REPLICAS_AFTER_APPEND for whichever of its partitions are still unconfirmed — the data IS on the leader; what timed out is confirmation, not the write itself. */
    @Override
    public void tick() {
        long now = System.currentTimeMillis();
        Iterator<PendingProduce> it = pending.iterator();
        while (it.hasNext()) {
            PendingProduce p = it.next();
            if (p.deadlineMillis() > now) {
                continue;
            }
            it.remove();
            List<TopicResult> finalResults = applyTimeoutErrors(p.results(), p.waits());
            p.context().sendAsync(writeResponse(finalResults));
        }
    }

    private List<TopicResult> applyTimeoutErrors(List<TopicResult> results, List<AckWait> waits) {
        List<TopicResult> patched = new ArrayList<>(results.size());
        for (TopicResult topic : results) {
            List<PartitionResult> partitions = new ArrayList<>(topic.partitions().size());
            for (PartitionResult partition : topic.partitions()) {
                boolean stillWaiting = wasWaitingAndUnsatisfied(topic.name(), partition, waits);
                partitions.add(stillWaiting
                        ? new PartitionResult(partition.index(), Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND, partition.baseOffset(), partition.recordCount())
                        : partition);
            }
            patched.add(new TopicResult(topic.name(), partitions));
        }
        return patched;
    }

    private boolean wasWaitingAndUnsatisfied(String topic, PartitionResult partition, List<AckWait> waits) {
        for (AckWait w : waits) {
            if (w.topic().equals(topic) && w.partition() == partition.index()) {
                return replicaManager.highWaterMark(topic, partition.index()) < w.targetOffsetExclusive();
            }
        }
        return false;
    }

    private record PartitionResult(int index, short errorCode, long baseOffset, int recordCount) {}
    private record TopicResult(String name, List<PartitionResult> partitions) {}

    private TopicResult readTopicData(ProtocolReader reader, String transactionalId) {
        String topicName = reader.readString();
        List<PartitionResult> partitionResults = reader.readArray(r -> readPartitionData(r, topicName, transactionalId));
        return new TopicResult(topicName, partitionResults);
    }

    private PartitionResult readPartitionData(ProtocolReader reader, String topicName, String transactionalId) {
        int index = reader.readInt32();
        byte[] recordsBytes = reader.readNullableBytes();

        if (transactionalId != null) {
            // Transactions are a non-goal (PRD §2) — we understand what they're
            // for but don't implement them, so we reject rather than silently
            // treating a transactional produce as a normal one.
            return new PartitionResult(index, Errors.INVALID_REQUEST, -1, 0);
        }

        java.util.Optional<PartitionLog> partitionLog = logManager.getPartition(topicName, index);
        if (partitionLog.isEmpty()) {
            return new PartitionResult(index, Errors.UNKNOWN_TOPIC_OR_PARTITION, -1, 0);
        }
        if (!replicaManager.isLeaderFor(topicName, index)) {
            // PRD §8.5: reject before ever touching the log — a non-leader
            // (including a fenced, formerly-current leader) accepting a
            // write locally is exactly the divergence §8.3's truncation
            // exists to clean up after the fact; better to never create it.
            return new PartitionResult(index, Errors.NOT_LEADER_OR_FOLLOWER, -1, 0);
        }
        if (recordsBytes == null) {
            return new PartitionResult(index, Errors.CORRUPT_MESSAGE, -1, 0);
        }

        try {
            RecordBatch batch = RecordBatch.parse(recordsBytes);
            // PRD §8.5: stamp the epoch this batch was actually accepted
            // under into the on-disk record (the field M1's §5.4 already
            // reserved for this) — the same moment, and the same
            // "post-hoc patch a producer-sent placeholder" idea, as
            // rewriteBaseOffset() right below.
            RecordBatch.rewritePartitionLeaderEpoch(recordsBytes, replicaManager.currentEpoch(topicName, index));
            long baseOffset = partitionLog.get().append(recordsBytes, batch.recordCount());
            replicaManager.afterLeaderAppend(topicName, index);
            fetchCompletionListener.onAppended(topicName, index);
            return new PartitionResult(index, Errors.NONE, baseOffset, batch.recordCount());
        } catch (RecordBatch.InvalidRecordBatchException e) {
            return new PartitionResult(index, e.errorCode, -1, 0);
        }
    }

    private static byte[] writeResponse(List<TopicResult> results) {
        ProtocolWriter response = new ProtocolWriter();
        response.writeArray(results, ProduceHandler::writeTopicResult);
        response.writeInt32(0); // throttle_time_ms
        return response.toByteArray();
    }

    private static void writeTopicResult(ProtocolWriter w, TopicResult result) {
        w.writeString(result.name());
        w.writeArray(result.partitions(), (pw, partition) -> {
            pw.writeInt32(partition.index());
            pw.writeInt16(partition.errorCode());
            pw.writeInt64(partition.baseOffset());
            pw.writeInt64(-1); // log_append_time — M1 doesn't track broker-assigned append times
        });
    }
}
