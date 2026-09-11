package com.advaith.broker.api;

import com.advaith.broker.log.LogManager;
import com.advaith.broker.log.PartitionLog;
import com.advaith.broker.protocol.ApiKey;
import com.advaith.broker.protocol.Errors;
import com.advaith.broker.protocol.ProtocolReader;
import com.advaith.broker.protocol.ProtocolWriter;

import java.util.List;
import java.util.Optional;

/**
 * Reads record batches back from an offset (PRD §5.3). Design decision,
 * flagged here rather than hidden: real Kafka long-polls a Fetch — holding
 * the request open until min_bytes is available or max_wait_ms elapses.
 * M1 does neither; it answers immediately with whatever is already in the
 * log, which is only correct because our single-threaded handler never
 * blocks anyway. Proper long-polling needs a way to park a request without
 * blocking the selector thread, which is exactly the machinery M3 adds
 * alongside consumer groups.
 */
public final class FetchHandler implements ApiHandler {

    private final LogManager logManager;

    public FetchHandler(LogManager logManager) {
        this.logManager = logManager;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.FETCH;
    }

    @Override
    public byte[] handle(short apiVersion, ProtocolReader request) {
        request.readInt32(); // replica_id — -1 from a normal consumer; M1 has no followers to distinguish
        request.readInt32(); // max_wait_ms — ignored, see class javadoc
        request.readInt32(); // min_bytes — ignored, see class javadoc
        request.readInt32(); // max_bytes — the overall request-level cap; M1 only enforces the per-partition cap below
        request.readInt8();  // isolation_level — 0 = read_uncommitted is the only mode; transactions are a non-goal

        List<TopicResult> results = request.readArray(this::readTopic);

        ProtocolWriter response = new ProtocolWriter();
        response.writeInt32(0); // throttle_time_ms
        response.writeArray(results, FetchHandler::writeTopicResult);
        return response.toByteArray();
    }

    private record PartitionResult(int index, short errorCode, long highWatermark, byte[] records) {}
    private record TopicResult(String name, List<PartitionResult> partitions) {}

    private TopicResult readTopic(ProtocolReader reader) {
        String topicName = reader.readString();
        List<PartitionResult> partitions = reader.readArray(r -> readPartition(r, topicName));
        return new TopicResult(topicName, partitions);
    }

    private PartitionResult readPartition(ProtocolReader reader, String topicName) {
        int partitionIndex = reader.readInt32();
        long fetchOffset = reader.readInt64();
        int partitionMaxBytes = reader.readInt32();

        Optional<PartitionLog> partitionLog = logManager.getPartition(topicName, partitionIndex);
        if (partitionLog.isEmpty()) {
            return new PartitionResult(partitionIndex, Errors.UNKNOWN_TOPIC_OR_PARTITION, -1, null);
        }

        PartitionLog log = partitionLog.get();
        try {
            byte[] records = log.read(fetchOffset, partitionMaxBytes);
            return new PartitionResult(partitionIndex, Errors.NONE, log.logEndOffset(), records);
        } catch (PartitionLog.OffsetOutOfRangeException e) {
            return new PartitionResult(partitionIndex, Errors.OFFSET_OUT_OF_RANGE, log.logEndOffset(), null);
        }
    }

    private static void writeTopicResult(ProtocolWriter w, TopicResult result) {
        w.writeString(result.name());
        w.writeArray(result.partitions(), (pw, partition) -> {
            pw.writeInt32(partition.index());
            pw.writeInt16(partition.errorCode());
            pw.writeInt64(partition.highWatermark());
            // last_stable_offset only differs from high_watermark once transactions
            // can leave uncommitted records in between the two — a non-goal here
            // (PRD §2), so M1 reports the same value for both.
            pw.writeInt64(partition.highWatermark());
            pw.writeArray(null, (aw, ignored) -> {}); // aborted_transactions — PRD §5.3: "send null"
            pw.writeNullableBytes(partition.records());
        });
    }
}
