package com.advaith.broker.log;

import com.advaith.broker.record.RecordBatch;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

/**
 * One segment of one partition's log: a `.log` file of raw RecordBatch
 * bytes back-to-back, plus a sparse `.index` alongside it (PRD §6.2/§6.3).
 * Design decision: a segment is either the active one (still being
 * appended to) or closed (rolled, never appended to again) — that
 * distinction is what makes crash recovery only need to re-validate the
 * active segment (§6.4) and retention only ever delete closed ones (§6.5).
 */
final class LogSegment {

    private static final int FILENAME_DIGITS = 20;

    private final long baseOffset;
    private final Path logPath;
    private final Path indexPath;
    private final FileChannel logChannel;
    private final OffsetIndex index;
    private final int indexIntervalBytes;

    private long sizeBytes;
    private long bytesSinceLastIndexEntry;

    /**
     * The offset one past the last record this segment currently holds —
     * i.e. baseOffset if nothing's been appended yet, or (last batch's own
     * baseOffset + its recordCount) otherwise. PartitionLog reads this
     * exactly once, right after opening the active segment (fresh or
     * recovered), to know where to resume assigning offsets from. Never
     * meaningful on a closed segment — nothing ever needs it from one,
     * since PartitionLog's own running counter takes over from here.
     */
    private long nextOffsetAfterContents;

    private LogSegment(long baseOffset, Path logPath, Path indexPath, FileChannel logChannel,
                        OffsetIndex index, long sizeBytes, int indexIntervalBytes) {
        this.baseOffset = baseOffset;
        this.logPath = logPath;
        this.indexPath = indexPath;
        this.logChannel = logChannel;
        this.index = index;
        this.sizeBytes = sizeBytes;
        this.indexIntervalBytes = indexIntervalBytes;
    }

    /**
     * Segment files are named by base offset, zero-padded to 20 digits —
     * real Kafka's actual convention, not a simplification. It's what
     * makes "which segment holds offset N" a filename/sort comparison
     * instead of needing a separate manifest file to track it.
     */
    private static String fileStemFor(long baseOffset) {
        return String.format("%0" + FILENAME_DIGITS + "d", baseOffset);
    }

    static Path logPathFor(Path dir, long baseOffset) {
        return dir.resolve(fileStemFor(baseOffset) + ".log");
    }

    static Path indexPathFor(Path dir, long baseOffset) {
        return dir.resolve(fileStemFor(baseOffset) + ".index");
    }

    /** Brand-new, empty segment — used when a partition starts empty, or right after rolling. */
    static LogSegment createNew(Path dir, long baseOffset, int indexIntervalBytes) throws IOException {
        Path logPath = logPathFor(dir, baseOffset);
        Path indexPath = indexPathFor(dir, baseOffset);
        FileChannel channel = FileChannel.open(logPath,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE);
        OffsetIndex index = OffsetIndex.createEmpty(indexPath);
        LogSegment segment = new LogSegment(baseOffset, logPath, indexPath, channel, index, 0, indexIntervalBytes);
        segment.nextOffsetAfterContents = baseOffset; // empty: nothing appended yet
        return segment;
    }

    /**
     * Opens a segment that was closed by a previous roll. Trusted as-is —
     * its length and index are final, because nothing appends to a closed
     * segment ever again (PRD §6.4).
     */
    static LogSegment openClosed(Path dir, long baseOffset, int indexIntervalBytes) throws IOException {
        Path logPath = logPathFor(dir, baseOffset);
        Path indexPath = indexPathFor(dir, baseOffset);
        FileChannel channel = FileChannel.open(logPath, StandardOpenOption.READ, StandardOpenOption.WRITE);
        OffsetIndex index = OffsetIndex.loadExisting(indexPath);
        return new LogSegment(baseOffset, logPath, indexPath, channel, index, channel.size(), indexIntervalBytes);
    }

    /**
     * Opens the LAST segment after a restart — the one that may have been
     * mid-write when the broker stopped, cleanly or not. Unlike
     * openClosed(), this always re-validates every batch from the start
     * and truncates at the first sign of a torn write (PRD §6.4): an
     * unclean shutdown could have caught a write() in progress (an
     * incomplete batch at the tail — too short to even claim a full
     * header) or left a batch whose bytes made it into the file but fail
     * CRC (a write that completed at the byte level but was never fsync'd
     * before power loss, so what actually landed on the physical disk
     * doesn't match what was written). Both get the same treatment:
     * discard everything from that point on. Never invent data, and never
     * trust a byte range you haven't re-verified since the last clean
     * shutdown.
     */
    static LogSegment recoverAsActive(Path dir, long baseOffset, int indexIntervalBytes) throws IOException {
        Path logPath = logPathFor(dir, baseOffset);
        Path indexPath = indexPathFor(dir, baseOffset);
        FileChannel channel = FileChannel.open(logPath, StandardOpenOption.READ, StandardOpenOption.WRITE);

        long fileSize = channel.size();
        ByteBuffer wholeFile = ByteBuffer.allocate((int) fileSize);
        channel.read(wholeFile, 0);
        byte[] bytes = wholeFile.array();

        long validBytes = 0;
        long recoveredNextOffset = baseOffset; // updated as each batch validates; stays at baseOffset if the segment is empty
        while (fileSize - validBytes >= RecordBatch.FIXED_HEADER_SIZE) {
            RecordBatch.BatchLocation location = RecordBatch.peekLocation(bytes, (int) validBytes);
            if (location.totalSizeInBytes() <= 0 || validBytes + location.totalSizeInBytes() > fileSize) {
                break; // claims to extend past what's actually on disk: a torn write
            }
            byte[] batchBytes = Arrays.copyOfRange(bytes, (int) validBytes, (int) validBytes + location.totalSizeInBytes());
            RecordBatch parsed;
            try {
                parsed = RecordBatch.parse(batchBytes); // full validation, including CRC — see javadoc above
            } catch (RecordBatch.InvalidRecordBatchException e) {
                break; // corrupt tail: the bytes are present but don't check out
            }
            recoveredNextOffset = location.baseOffset() + parsed.recordCount();
            validBytes += location.totalSizeInBytes();
        }

        if (validBytes < fileSize) {
            channel.truncate(validBytes);
        }

        // The index is always a rebuildable cache, never a source of truth
        // (see OffsetIndex's javadoc) — cheapest correct move here is to
        // throw away whatever was on disk and rebuild it fresh from the
        // now-verified log bytes, rather than validating the old index.
        Files.deleteIfExists(indexPath);
        OffsetIndex index = OffsetIndex.createEmpty(indexPath);
        LogSegment segment = new LogSegment(baseOffset, logPath, indexPath, channel, index, validBytes, indexIntervalBytes);
        segment.nextOffsetAfterContents = recoveredNextOffset;
        segment.rebuildIndex(bytes, validBytes);
        return segment;
    }

    long nextOffsetAfterContents() {
        return nextOffsetAfterContents;
    }

    private void rebuildIndex(byte[] bytes, long validBytes) throws IOException {
        long position = 0;
        while (position < validBytes) {
            RecordBatch.BatchLocation location = RecordBatch.peekLocation(bytes, (int) position);
            maybeAddIndexEntry(location.baseOffset(), (int) position, location.totalSizeInBytes());
            position += location.totalSizeInBytes();
        }
    }

    long baseOffset() {
        return baseOffset;
    }

    long sizeBytes() {
        return sizeBytes;
    }

    /** Appends one already-offset-assigned batch. The caller (PartitionLog) already knows the offset it assigned; this just persists it. */
    void append(byte[] batchBytes, long batchBaseOffset, int recordCount) throws IOException {
        int position = (int) sizeBytes;
        logChannel.write(ByteBuffer.wrap(batchBytes), position);
        maybeAddIndexEntry(batchBaseOffset, position, batchBytes.length);
        sizeBytes += batchBytes.length;
        nextOffsetAfterContents = batchBaseOffset + recordCount;
    }

    private void maybeAddIndexEntry(long batchBaseOffset, int position, int batchSize) throws IOException {
        bytesSinceLastIndexEntry += batchSize;
        if (bytesSinceLastIndexEntry >= indexIntervalBytes) {
            index.append((int) (batchBaseOffset - baseOffset), position);
            bytesSinceLastIndexEntry = 0;
        }
    }

    /** Forces the log file's bytes out of the OS page cache and onto physical storage — the actual durability guarantee (PRD §6.6). */
    void flush() throws IOException {
        logChannel.force(true);
    }

    /**
     * Reads from fromOffset (an absolute offset, not relative to this
     * segment) onward, returning whole batches up to maxBytes — the exact
     * same contract as M1's in-memory PartitionLog.read(), just sourced
     * from a file instead of a List.
     */
    byte[] read(long fromOffset, int maxBytes) throws IOException {
        int approxStart = index.lookup((int) (fromOffset - baseOffset));

        ByteBuffer fileContents = ByteBuffer.allocate((int) (sizeBytes - approxStart));
        logChannel.read(fileContents, approxStart);
        byte[] bytes = fileContents.array();

        // Phase 1: the index only got us to a position AT OR BEFORE the
        // target, not necessarily the exact containing batch. Batches are
        // appended in strictly increasing, non-overlapping offset order,
        // so the batch that actually contains fromOffset is the last one
        // whose own baseOffset is <= fromOffset — walk forward from the
        // index's approximate position until the next batch would
        // overshoot it, and remember the last one that didn't.
        int collectFrom = 0;
        int position = 0;
        while (position < bytes.length) {
            RecordBatch.BatchLocation location = RecordBatch.peekLocation(bytes, position);
            if (location.baseOffset() > fromOffset) {
                break;
            }
            collectFrom = position;
            position += location.totalSizeInBytes();
        }

        // Phase 2: collect whole batches from there, bounded by maxBytes —
        // except the first is always included even if it alone exceeds the
        // budget, so a fetch never wedges behind one oversized batch.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        position = collectFrom;
        while (position < bytes.length) {
            RecordBatch.BatchLocation location = RecordBatch.peekLocation(bytes, position);
            if (out.size() > 0 && out.size() + location.totalSizeInBytes() > maxBytes) {
                break;
            }
            out.write(bytes, position, location.totalSizeInBytes());
            position += location.totalSizeInBytes();
        }
        return out.toByteArray();
    }

    long lastModifiedMillis() throws IOException {
        return Files.getLastModifiedTime(logPath).toMillis();
    }

    void close() throws IOException {
        logChannel.close();
        index.close();
    }

    /** Deletes both files that make up this segment as one unit (PRD §6.5) — a segment is never partially deleted. */
    void deleteFiles() throws IOException {
        close();
        Files.deleteIfExists(logPath);
        Files.deleteIfExists(indexPath);
    }
}
