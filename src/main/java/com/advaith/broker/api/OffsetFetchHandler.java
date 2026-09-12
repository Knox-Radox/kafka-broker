package com.advaith.broker.api;

import com.advaith.broker.group.OffsetStore;
import com.advaith.broker.protocol.ApiKey;
import com.advaith.broker.protocol.Errors;
import com.advaith.broker.protocol.ProtocolReader;
import com.advaith.broker.protocol.ProtocolWriter;

import java.util.List;

/**
 * Retrieves a group's previously committed offsets (PRD §7.4). Not
 * generation-fenced (§7.5 names JoinGroup/SyncGroup/Heartbeat/OffsetCommit
 * specifically — reading a value can't cause the "stale writer" bug
 * fencing exists to prevent, only writing one can), and a partition with
 * no prior commit is NOT an error — same convention ListOffsetsHandler
 * already uses for "nothing found": offset -1, empty metadata.
 */
public final class OffsetFetchHandler implements ApiHandler {

    private final OffsetStore offsetStore;

    public OffsetFetchHandler(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.OFFSET_FETCH;
    }

    @Override
    public byte[] handle(RequestContext context, ProtocolReader request) {
        String groupId = request.readString();
        List<TopicResult> results = request.readArray(r -> readTopic(r, groupId));

        ProtocolWriter response = new ProtocolWriter();
        response.writeArray(results, OffsetFetchHandler::writeTopicResult);
        return response.toByteArray();
    }

    private record PartitionResult(int index, long offset, String metadata, short errorCode) {}
    private record TopicResult(String name, List<PartitionResult> partitions) {}

    private TopicResult readTopic(ProtocolReader reader, String groupId) {
        String topicName = reader.readString();
        List<Integer> partitionIndexes = reader.readArray(ProtocolReader::readInt32);
        List<PartitionResult> partitions = partitionIndexes.stream()
                .map(index -> lookup(groupId, topicName, index))
                .toList();
        return new TopicResult(topicName, partitions);
    }

    private PartitionResult lookup(String groupId, String topicName, int partitionIndex) {
        return offsetStore.fetch(groupId, topicName, partitionIndex)
                .map(committed -> new PartitionResult(partitionIndex, committed.offset(), committed.metadata(), Errors.NONE))
                .orElse(new PartitionResult(partitionIndex, -1, "", Errors.NONE));
    }

    private static void writeTopicResult(ProtocolWriter w, TopicResult result) {
        w.writeString(result.name());
        w.writeArray(result.partitions(), (pw, p) -> {
            pw.writeInt32(p.index());
            pw.writeInt64(p.offset());
            pw.writeNullableString(p.metadata());
            pw.writeInt16(p.errorCode());
        });
    }
}
