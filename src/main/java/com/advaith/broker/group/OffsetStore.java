package com.advaith.broker.group;

import com.advaith.broker.log.LogManager;
import com.advaith.broker.log.PartitionLog;
import com.advaith.broker.record.SimpleRecordCodec;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Durable storage for consumer group offset commits (PRD §7.4's
 * OffsetCommit/OffsetFetch): "store this exactly like a normal
 * topic-partition's data — an internal, broker-managed topic where each
 * commit is itself a record" — reusing the exact same PartitionLog M2
 * already built, rather than inventing a second storage mechanism.
 *
 * Design: every commit is appended, durably, to the internal
 * {@value GroupCoordinator#OFFSETS_TOPIC} topic (routed to a partition
 * computed from the group id — see {@link GroupCoordinator#coordinatorPartitionFor}
 * — so different groups' commits don't all serialize through one
 * partition). Reads never touch disk, though: like real Kafka's own group
 * coordinator, this class keeps a fully materialized in-memory view
 * ({@code committed}) that OffsetFetch reads from directly, rebuilt once
 * at startup by replaying every record in the internal topic. This is the
 * direct, honest answer to "why not just an in-memory map" — because an
 * in-memory map alone can't survive a restart, and a full log scan on
 * every OffsetFetch would be needlessly slow for something that changes
 * only on commit; a log that's also cached in memory gets both properties.
 * No compaction is implemented (a real, separate feature this project
 * doesn't need at M3's scale) — the internal topic simply keeps every
 * commit ever made, same retention rules as any other topic.
 */
public final class OffsetStore {

    /** Separates the three parts of an internal record's key. Not valid in a real Kafka topic/group name, so it can't collide with a caller's own data. */
    private static final char KEY_SEPARATOR = '\u0001';

    public record CommittedOffset(long offset, String metadata) {}

    private final LogManager logManager;
    private final int partitionCount;

    // groupId -> topic -> partition -> committed offset. Single selector
    // thread only touches this (same invariant as everything else in this
    // codebase), so a plain HashMap is correct without synchronization.
    private final Map<String, Map<String, Map<Integer, CommittedOffset>>> committed = new HashMap<>();

    public OffsetStore(LogManager logManager, int partitionCount) {
        this.logManager = logManager;
        this.partitionCount = partitionCount;
        loadFromDisk();
    }

    private void loadFromDisk() {
        for (int p = 0; p < partitionCount; p++) {
            int partition = p; // effectively-final copy for the lambda below
            PartitionLog log = logManager.getPartition(GroupCoordinator.OFFSETS_TOPIC, partition)
                    .orElseThrow(() -> new IllegalStateException(
                            "internal topic " + GroupCoordinator.OFFSETS_TOPIC + " partition " + partition + " was not registered with LogManager"));
            long cursor = log.logStartOffset();
            while (cursor < log.logEndOffset()) {
                // A generous chunk size — the internal offsets topic is
                // expected to stay small at this project's scale — read in
                // a loop rather than one giant read so this doesn't assume
                // the whole topic fits any particular size at all.
                byte[] chunk = log.read(cursor, 8 * 1024 * 1024);
                if (chunk.length == 0) {
                    break; // shouldn't happen given the cursor < logEndOffset guard, but never loop forever on a read that returned nothing
                }
                for (SimpleRecordCodec.Decoded record : SimpleRecordCodec.decodeAll(chunk)) {
                    applyDecodedCommit(record);
                    cursor++; // every commit record here is its own single-record batch (see SimpleRecordCodec), so each one advances the offset by exactly 1
                }
            }
        }
    }

    private void applyDecodedCommit(SimpleRecordCodec.Decoded record) {
        String key = new String(record.key(), StandardCharsets.UTF_8);
        String[] parts = key.split(String.valueOf(KEY_SEPARATOR), 3);
        String groupId = parts[0];
        String topic = parts[1];
        int partition = Integer.parseInt(parts[2]);

        long offset = ByteReader.readLong(record.value());
        String metadata = ByteReader.readTrailingNullableString(record.value());

        committed.computeIfAbsent(groupId, g -> new HashMap<>())
                .computeIfAbsent(topic, t -> new HashMap<>())
                .put(partition, new CommittedOffset(offset, metadata));
    }

    public void commit(String groupId, String topic, int partition, long offset, String metadata) {
        byte[] key = (groupId + KEY_SEPARATOR + topic + KEY_SEPARATOR + partition).getBytes(StandardCharsets.UTF_8);
        byte[] value = ByteWriter.longAndNullableString(offset, metadata);
        byte[] batch = SimpleRecordCodec.buildSingleRecordBatch(key, value);

        PartitionLog log = logManager.getPartition(GroupCoordinator.OFFSETS_TOPIC,
                        GroupCoordinator.coordinatorPartitionFor(groupId, partitionCount))
                .orElseThrow();
        log.append(batch, 1);

        committed.computeIfAbsent(groupId, g -> new HashMap<>())
                .computeIfAbsent(topic, t -> new HashMap<>())
                .put(partition, new CommittedOffset(offset, metadata));
    }

    public Optional<CommittedOffset> fetch(String groupId, String topic, int partition) {
        return Optional.ofNullable(committed
                .getOrDefault(groupId, Map.of())
                .getOrDefault(topic, Map.of())
                .get(partition));
    }

    // Tiny local helpers — not worth pulling in ProtocolReader/Writer for a value
    // this shaped (a fixed int64 followed by one nullable string), since this
    // is an internal-only encoding no client ever parses.

    private static final class ByteWriter {
        static byte[] longAndNullableString(long value, String s) {
            byte[] strBytes = s == null ? null : s.getBytes(StandardCharsets.UTF_8);
            int len = strBytes == null ? 0 : strBytes.length;
            java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(8 + 4 + len);
            buf.putLong(value);
            buf.putInt(strBytes == null ? -1 : len);
            if (strBytes != null) {
                buf.put(strBytes);
            }
            return buf.array();
        }
    }

    private static final class ByteReader {
        static long readLong(byte[] value) {
            return java.nio.ByteBuffer.wrap(value).getLong(0);
        }

        static String readTrailingNullableString(byte[] value) {
            java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(value);
            buf.position(8);
            int len = buf.getInt();
            if (len < 0) {
                return null;
            }
            byte[] bytes = new byte[len];
            buf.get(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }
}
