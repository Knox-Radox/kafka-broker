package com.advaith.broker.log;

import com.advaith.broker.record.RecordBatch;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * M1 storage for one topic-partition: an in-memory, append-only list of raw
 * RecordBatch byte blobs (durable files are M2's job — PRD §5.1). Design
 * decision: deliberately NOT thread-safe. M1's single selector thread is
 * the only caller; whatever eventually makes the broker multi-threaded
 * (M4's replication is the likely candidate) must add synchronization here
 * first, not assume it already exists.
 */
public final class PartitionLog {

    /** A fetch asked for an offset already discarded from the log — in M1 this only fires for negative offsets, since nothing is ever actually discarded yet. */
    public static final class OffsetOutOfRangeException extends RuntimeException {
        public OffsetOutOfRangeException(String message) {
            super(message);
        }
    }

    private record StoredBatch(long baseOffset, long lastOffset, byte[] bytes) {}

    private final List<StoredBatch> batches = new ArrayList<>();

    /** The offset that will be assigned to the next appended record. Only this class may advance it. */
    private long nextOffset = 0;

    /**
     * Assigns the batch the next contiguous offset range, patches its
     * baseOffset field to match, and appends it. Returns the assigned base
     * offset, which the Produce response must report back to the client
     * verbatim (PRD §5.3) — it's how the producer learns what offset its
     * first record actually landed at.
     */
    public long append(byte[] rawBatchBytes, int recordCount) {
        long assignedBaseOffset = nextOffset;
        RecordBatch.rewriteBaseOffset(rawBatchBytes, assignedBaseOffset);
        long lastOffset = assignedBaseOffset + recordCount - 1;
        batches.add(new StoredBatch(assignedBaseOffset, lastOffset, rawBatchBytes));
        nextOffset += recordCount;
        return assignedBaseOffset;
    }

    /**
     * The offset one past the last record ever appended — i.e. where the
     * next append will land. In M1 this doubles as the partition's
     * high-water mark: with no replicas to lag behind, everything appended
     * is immediately "committed" (M4 is where these two concepts split).
     */
    public long logEndOffset() {
        return nextOffset;
    }

    /** Always 0 in M1 — nothing is ever deleted, so the log's start never moves. M2's retention is what makes this non-trivial. */
    public long logStartOffset() {
        return 0;
    }

    /**
     * Concatenates raw batch bytes starting from the batch containing
     * fromOffset, up to maxBytes. The first qualifying batch is always
     * included whole even if it alone exceeds maxBytes — otherwise a
     * consumer with a small max_bytes ahead of one large message could
     * never make progress. Returns an empty (non-null) array, not an
     * error, if fromOffset has already caught up to the tail.
     */
    public byte[] read(long fromOffset, int maxBytes) {
        if (fromOffset < logStartOffset()) {
            throw new OffsetOutOfRangeException(
                    "requested offset " + fromOffset + " is before log start " + logStartOffset());
        }
        if (fromOffset >= logEndOffset()) {
            return new byte[0]; // caught up to the tail — PRD §5.3: no error for this case
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (StoredBatch batch : batches) {
            if (batch.lastOffset() < fromOffset) {
                continue; // entirely before what was asked for
            }
            if (out.size() > 0 && out.size() + batch.bytes().length > maxBytes) {
                break;
            }
            out.writeBytes(batch.bytes());
        }
        return out.toByteArray();
    }
}
