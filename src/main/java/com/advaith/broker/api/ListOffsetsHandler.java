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
 * Resolves "earliest"/"latest" to concrete offsets (PRD §5.3) — this is how
 * a consumer turns `--from-beginning` or "give me new records only" into an
 * actual number to Fetch from. Design decision, an explicit scope cut: real
 * Kafka can also resolve an arbitrary wall-clock timestamp to "the first
 * offset at or after that time," using a per-segment time index. M1 has no
 * segments and no time index (that's M2), so only the two sentinel
 * timestamps are implemented; anything else reports "no match" rather than
 * pretending to search.
 */
public final class ListOffsetsHandler implements ApiHandler {

    private static final long EARLIEST = -2;
    private static final long LATEST = -1;

    private final LogManager logManager;

    public ListOffsetsHandler(LogManager logManager) {
        this.logManager = logManager;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.LIST_OFFSETS;
    }

    @Override
    public byte[] handle(short apiVersion, ProtocolReader request) {
        request.readInt32(); // replica_id — always -1 from a normal consumer in M1

        List<TopicResult> results = request.readArray(this::readTopic);

        ProtocolWriter response = new ProtocolWriter();
        // NOTE: no throttle_time_ms here, despite the PRD's §5.3 transcription
        // listing one for v1. Verified empirically against a real client
        // (see JOURNAL.md, 2026-09-11): throttle_time_ms was added to
        // ListOffsetsResponse in a later version than v1, and writing it
        // anyway makes a real client misread it as the topics array's own
        // element count (4 zero bytes -> "0 topics"), silently swallowing
        // every result without throwing anything on either side.
        response.writeArray(results, ListOffsetsHandler::writeTopicResult);
        return response.toByteArray();
    }

    private record PartitionResult(int index, short errorCode, long timestamp, long offset) {}
    private record TopicResult(String name, List<PartitionResult> partitions) {}

    private TopicResult readTopic(ProtocolReader reader) {
        String topicName = reader.readString();
        List<PartitionResult> partitions = reader.readArray(r -> readPartition(r, topicName));
        return new TopicResult(topicName, partitions);
    }

    private PartitionResult readPartition(ProtocolReader reader, String topicName) {
        int partitionIndex = reader.readInt32();
        long requestedTimestamp = reader.readInt64();

        Optional<PartitionLog> partitionLog = logManager.getPartition(topicName, partitionIndex);
        if (partitionLog.isEmpty()) {
            return new PartitionResult(partitionIndex, Errors.UNKNOWN_TOPIC_OR_PARTITION, -1, -1);
        }
        PartitionLog log = partitionLog.get();

        if (requestedTimestamp == EARLIEST) {
            return new PartitionResult(partitionIndex, Errors.NONE, -1, log.logStartOffset());
        }
        if (requestedTimestamp == LATEST) {
            return new PartitionResult(partitionIndex, Errors.NONE, -1, log.logEndOffset());
        }
        // Arbitrary timestamp lookup needs a time index we don't have in M1
        // (see class javadoc). Real Kafka's own convention for "nothing
        // found" is offset=-1 with error_code=NONE — that's what we report
        // here too, rather than a made-up error code for an unimplemented
        // feature.
        return new PartitionResult(partitionIndex, Errors.NONE, -1, -1);
    }

    private static void writeTopicResult(ProtocolWriter w, TopicResult result) {
        w.writeString(result.name());
        w.writeArray(result.partitions(), (pw, partition) -> {
            pw.writeInt32(partition.index());
            pw.writeInt16(partition.errorCode());
            pw.writeInt64(partition.timestamp());
            pw.writeInt64(partition.offset());
        });
    }
}
