package com.advaith.broker.log;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * Sparse offset index for one segment (PRD §6.2): fixed 8-byte entries of
 * (relativeOffset INT32, physicalPosition INT32), appended only every
 * `indexIntervalBytes` of log data written — not per record — so the index
 * stays a small fraction of the log's own size. Design decision: entries
 * are kept fully in memory (the whole point of "sparse" is that this list
 * stays small) and only written through to disk for the next restart;
 * on recovery, the index is rebuilt from the log itself anyway (see
 * LogSegment), so this file is a cache, never the source of truth.
 */
final class OffsetIndex {

    private static final int ENTRY_SIZE = 8; // INT32 relativeOffset + INT32 position

    private final FileChannel channel;
    private final List<int[]> entries = new ArrayList<>(); // each: {relativeOffset, position}, ascending

    private OffsetIndex(FileChannel channel) {
        this.channel = channel;
    }

    static OffsetIndex createEmpty(Path path) throws IOException {
        FileChannel channel = FileChannel.open(path,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.READ, StandardOpenOption.WRITE);
        return new OffsetIndex(channel);
    }

    static OffsetIndex loadExisting(Path path) throws IOException {
        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        OffsetIndex index = new OffsetIndex(channel);
        int size = (int) channel.size();
        ByteBuffer buf = ByteBuffer.allocate(size);
        channel.read(buf, 0);
        buf.flip();
        while (buf.remaining() >= ENTRY_SIZE) {
            int relativeOffset = buf.getInt();
            int position = buf.getInt();
            index.entries.add(new int[] {relativeOffset, position});
        }
        return index;
    }

    void append(int relativeOffset, int position) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(ENTRY_SIZE);
        buf.putInt(relativeOffset);
        buf.putInt(position);
        buf.flip();
        // Positional write: doesn't disturb any stream-style position on
        // the channel, and channel.size() is always exactly "append here"
        // since entries are only ever added, never rewritten in place.
        channel.write(buf, channel.size());
        entries.add(new int[] {relativeOffset, position});
    }

    /**
     * Binary search for the entry with the largest relativeOffset that is
     * still <= target — the nearest indexed position at or before what was
     * asked for. Returns 0 (the start of the segment) if the index has no
     * entry that low yet, which is always a safe starting point: offset 0
     * relative to a segment is that segment's own first batch.
     */
    int lookup(int targetRelativeOffset) {
        int lo = 0;
        int hi = entries.size() - 1;
        int bestPosition = 0;
        while (lo <= hi) {
            int mid = (lo + hi) / 2;
            int[] entry = entries.get(mid);
            if (entry[0] <= targetRelativeOffset) {
                bestPosition = entry[1];
                lo = mid + 1; // there might be an even-closer entry further right
            } else {
                hi = mid - 1;
            }
        }
        return bestPosition;
    }

    /**
     * Drops every entry pointing at or past {@code maxPositionExclusive}
     * and rewrites the file to match — the index-side half of M4's log
     * truncation on divergence (PRD §8.3): entries before the cutoff stay
     * exactly as they were (still valid, still pointing at real batches
     * that survive the truncation), so this only ever shrinks the file,
     * never needs a full rebuild the way crash recovery does.
     */
    void truncateToPosition(int maxPositionExclusive) throws IOException {
        entries.removeIf(entry -> entry[1] >= maxPositionExclusive);
        channel.truncate(0);
        long writePosition = 0;
        for (int[] entry : entries) {
            ByteBuffer buf = ByteBuffer.allocate(ENTRY_SIZE);
            buf.putInt(entry[0]);
            buf.putInt(entry[1]);
            buf.flip();
            channel.write(buf, writePosition);
            writePosition += ENTRY_SIZE;
        }
    }

    void close() throws IOException {
        channel.close();
    }
}
