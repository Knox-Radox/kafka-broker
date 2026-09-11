package com.advaith.broker.api;

import com.advaith.broker.log.LogManager;
import com.advaith.broker.log.PartitionLog;
import com.advaith.broker.protocol.ApiKey;
import com.advaith.broker.protocol.Errors;
import com.advaith.broker.protocol.ProtocolReader;
import com.advaith.broker.protocol.ProtocolWriter;
import com.advaith.broker.record.RecordBatch;

import java.util.List;

/**
 * Writes record batches to the log (PRD §5.3). Two things in this class are
 * easy to get subtly wrong and both are called out explicitly in the spec:
 * (1) base_offset in the response must be what the log actually assigned,
 * never anything the producer sent, and (2) acks=0 means literally no
 * response at all — not an empty one, none — which this handler expresses
 * by returning null and letting RequestDispatcher's contract handle it.
 */
public final class ProduceHandler implements ApiHandler {

    private final LogManager logManager;

    public ProduceHandler(LogManager logManager) {
        this.logManager = logManager;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.PRODUCE;
    }

    @Override
    public byte[] handle(short apiVersion, ProtocolReader request) {
        String transactionalId = request.readNullableString();
        short acks = request.readInt16();
        request.readInt32(); // timeout_ms — M1 always answers immediately; there's nothing to time out on

        List<TopicResult> results = request.readArray(r -> readTopicData(r, transactionalId));

        if (acks == 0) {
            return null; // PRD §5.3: acks=0 means the client expects no response at all
        }

        ProtocolWriter response = new ProtocolWriter();
        response.writeArray(results, ProduceHandler::writeTopicResult);
        response.writeInt32(0); // throttle_time_ms
        return response.toByteArray();
    }

    private record PartitionResult(int index, short errorCode, long baseOffset) {}
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
            return new PartitionResult(index, Errors.INVALID_REQUEST, -1);
        }

        java.util.Optional<PartitionLog> partitionLog = logManager.getPartition(topicName, index);
        if (partitionLog.isEmpty()) {
            return new PartitionResult(index, Errors.UNKNOWN_TOPIC_OR_PARTITION, -1);
        }
        if (recordsBytes == null) {
            return new PartitionResult(index, Errors.CORRUPT_MESSAGE, -1);
        }

        try {
            RecordBatch batch = RecordBatch.parse(recordsBytes);
            long baseOffset = partitionLog.get().append(recordsBytes, batch.recordCount());
            return new PartitionResult(index, Errors.NONE, baseOffset);
        } catch (RecordBatch.InvalidRecordBatchException e) {
            return new PartitionResult(index, e.errorCode, -1);
        }
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
