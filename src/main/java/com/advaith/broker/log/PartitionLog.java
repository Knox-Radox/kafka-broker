package com.advaith.broker.log;

import com.advaith.broker.record.RecordBatch;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * File-backed storage for one topic-partition (M2 — PRD §6, replacing M1's
 * in-memory List<byte[]>). Design decision: the public contract
 * (append/read/logEndOffset/logStartOffset, even the exception type) is
 * IDENTICAL to M1's version on purpose — nothing above the storage boundary
 * (§4) needs to change at all; only where the bytes live changes. Still
 * deliberately NOT thread-safe, for the same reason as M1: one selector
 * thread is the only caller.
 */
public final class PartitionLog {

    public static final class OffsetOutOfRangeException extends RuntimeException {
        public OffsetOutOfRangeException(String message) {
            super(message);
        }
    }

    private final Path dir;
    private final StorageConfig config;

    /** Ascending by baseOffset; the last element is always the active (currently-appended-to) segment. */
    private final List<LogSegment> segments = new ArrayList<>();

    private long nextOffset;
    private int appendsSinceFlush;
    private long lastFlushAtMillis;

    public PartitionLog(Path dir, StorageConfig config) {
        this.dir = dir;
        this.config = config;
        try {
            Files.createDirectories(dir);
            List<Long> existingBaseOffsets = listExistingSegmentBaseOffsets();

            if (existingBaseOffsets.isEmpty()) {
                segments.add(LogSegment.createNew(dir, 0, config.indexIntervalBytes()));
            } else {
                // Every segment except the last was closed by a prior roll
                // and is trusted as-is; the last one may have been mid-write
                // when the broker last stopped, so it always gets
                // re-validated (PRD §6.4).
                for (int i = 0; i < existingBaseOffsets.size() - 1; i++) {
                    segments.add(LogSegment.openClosed(dir, existingBaseOffsets.get(i), config.indexIntervalBytes()));
                }
                long lastBaseOffset = existingBaseOffsets.get(existingBaseOffsets.size() - 1);
                segments.add(LogSegment.recoverAsActive(dir, lastBaseOffset, config.indexIntervalBytes()));
            }

            this.nextOffset = activeSegment().nextOffsetAfterContents();
            this.lastFlushAtMillis = System.currentTimeMillis();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to open partition log at " + dir, e);
        }
    }

    private List<Long> listExistingSegmentBaseOffsets() throws IOException {
        List<Long> baseOffsets = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.log")) {
            for (Path path : stream) {
                String fileName = path.getFileName().toString();
                String stem = fileName.substring(0, fileName.length() - ".log".length());
                baseOffsets.add(Long.parseLong(stem));
            }
        }
        Collections.sort(baseOffsets);
        return baseOffsets;
    }

    private LogSegment activeSegment() {
        return segments.get(segments.size() - 1);
    }

    /**
     * Assigns the batch the next contiguous offset range, patches its
     * baseOffset field to match, rolls to a new segment first if the active
     * one is already full, appends, applies the configured flush policy,
     * and finally checks retention. Returns the assigned base offset.
     */
    public long append(byte[] rawBatchBytes, int recordCount) {
        long assignedBaseOffset = nextOffset;
        RecordBatch.rewriteBaseOffset(rawBatchBytes, assignedBaseOffset);

        try {
            if (activeSegment().sizeBytes() >= config.segmentBytes()) {
                roll(assignedBaseOffset);
            }
            activeSegment().append(rawBatchBytes, assignedBaseOffset, recordCount);
            nextOffset += recordCount;

            maybeFlush();
            enforceRetention();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to append to partition log at " + dir, e);
        }

        return assignedBaseOffset;
    }

    private void roll(long newBaseOffset) throws IOException {
        // The old active segment is now closed forever — no more appends to
        // it — so it's safe for retention to delete it later, and it never
        // needs re-validating again on a future restart (PRD §6.3/§6.4).
        activeSegment().flush();
        segments.add(LogSegment.createNew(dir, newBaseOffset, config.indexIntervalBytes()));
    }

    private void maybeFlush() throws IOException {
        appendsSinceFlush++;
        boolean messageThresholdHit = appendsSinceFlush >= config.flushIntervalMessages();
        boolean timeThresholdHit = config.flushIntervalMs() != StorageConfig.UNLIMITED
                && System.currentTimeMillis() - lastFlushAtMillis >= config.flushIntervalMs();
        if (messageThresholdHit || timeThresholdHit) {
            activeSegment().flush();
            appendsSinceFlush = 0;
            lastFlushAtMillis = System.currentTimeMillis();
        }
    }

    private void enforceRetention() throws IOException {
        enforceSizeRetention();
        enforceAgeRetention();
    }

    private void enforceSizeRetention() throws IOException {
        if (config.retentionBytes() == StorageConfig.UNLIMITED) {
            return;
        }
        while (segments.size() > 1 && totalSizeBytes() > config.retentionBytes()) {
            deleteOldestSegment();
        }
    }

    private void enforceAgeRetention() throws IOException {
        if (config.retentionMs() == StorageConfig.UNLIMITED) {
            return;
        }
        long cutoff = System.currentTimeMillis() - config.retentionMs();
        while (segments.size() > 1 && segments.get(0).lastModifiedMillis() < cutoff) {
            deleteOldestSegment();
        }
    }

    private long totalSizeBytes() {
        long total = 0;
        for (LogSegment segment : segments) {
            total += segment.sizeBytes();
        }
        return total;
    }

    private void deleteOldestSegment() throws IOException {
        // Never the active segment (the size>1 guard above already ensures
        // this can't be the last one) — a segment is deleted as a whole
        // unit, both files together, never partially (PRD §6.5).
        segments.remove(0).deleteFiles();
    }

    /**
     * The offset one past the last record ever appended. In M1 this doubled
     * as the high-water mark too; that's still accurate here for the same
     * reason — no replicas exist yet to lag behind (M4 is where these
     * split).
     */
    public long logEndOffset() {
        return nextOffset;
    }

    /** The base offset of the oldest segment still on disk — 0 until retention has ever deleted anything, whatever survives after that. */
    public long logStartOffset() {
        return segments.get(0).baseOffset();
    }

    /**
     * Concatenates raw batch bytes starting from the batch containing
     * fromOffset, up to maxBytes. Same contract as M1's version — the
     * first qualifying batch is always included whole even if it alone
     * exceeds maxBytes.
     */
    public byte[] read(long fromOffset, int maxBytes) {
        if (fromOffset < logStartOffset()) {
            throw new OffsetOutOfRangeException(
                    "requested offset " + fromOffset + " is before log start " + logStartOffset());
        }
        if (fromOffset >= logEndOffset()) {
            return new byte[0]; // caught up to the tail — no error for this case
        }

        try {
            // Find the segment whose range contains fromOffset: the last
            // one (scanning oldest to newest) whose own baseOffset is
            // still <= fromOffset. Reads never need to span into a later
            // segment in this implementation — max_bytes budgets in
            // practice are generous relative to one segment's remaining
            // data, and PRD's M1 simplification of "return whatever's
            // readily available" already permits an under-sized response.
            for (int i = segments.size() - 1; i >= 0; i--) {
                LogSegment segment = segments.get(i);
                if (segment.baseOffset() <= fromOffset) {
                    return segment.read(fromOffset, maxBytes);
                }
            }
            return new byte[0]; // unreachable given the logStartOffset() check above, but never throw from a read
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read partition log at " + dir, e);
        }
    }

    /**
     * Flushes and closes every segment's file handles. Used for a graceful
     * shutdown, and by tests that need to simulate "restart" by opening a
     * fresh PartitionLog against the same directory afterward — on a real,
     * unclean shutdown (a kill -9), nothing calls this at all, which is
     * exactly the scenario crash recovery (§6.4) exists to handle.
     */
    public void close() {
        try {
            for (LogSegment segment : segments) {
                segment.flush();
                segment.close();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to close partition log at " + dir, e);
        }
    }
}
