package com.advaith.broker.api;

import com.advaith.broker.group.GroupCoordinator;
import com.advaith.broker.group.OffsetStore;
import com.advaith.broker.protocol.ApiKey;
import com.advaith.broker.protocol.Errors;
import com.advaith.broker.protocol.ProtocolReader;
import com.advaith.broker.protocol.ProtocolWriter;

import java.util.List;

/**
 * Stores a group's consumed-up-to position per partition (PRD §7.4) —
 * exactly like a normal topic-partition's data, via {@link OffsetStore},
 * which reuses the same durable PartitionLog machinery M2 already built.
 * Generation-fenced (PRD §7.5): a commit bearing a stale generation is
 * rejected rather than written, which is the actual mechanism that stops
 * a member that's been reassigned away from a partition from still
 * quietly overwriting its committed offset.
 */
public final class OffsetCommitHandler implements ApiHandler {

    private final GroupCoordinator groupCoordinator;
    private final OffsetStore offsetStore;

    public OffsetCommitHandler(GroupCoordinator groupCoordinator, OffsetStore offsetStore) {
        this.groupCoordinator = groupCoordinator;
        this.offsetStore = offsetStore;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.OFFSET_COMMIT;
    }

    @Override
    public byte[] handle(RequestContext context, ProtocolReader request) {
        String groupId = request.readString();
        int generationId = request.readInt32();
        String memberId = request.readString();

        short fencingError = groupCoordinator.checkGeneration(groupId, generationId, memberId);

        List<TopicResult> results = request.readArray(r -> readTopic(r, groupId, fencingError));

        ProtocolWriter response = new ProtocolWriter();
        // No throttle_time_ms at this version (PRD §5.3's ListOffsets
        // lesson applied deliberately here, not just remembered as
        // trivia — see JOURNAL.md, 2026-09-11): OffsetCommitResponse v1's
        // real schema is just the Topics array, nothing else.
        response.writeArray(results, OffsetCommitHandler::writeTopicResult);
        return response.toByteArray();
    }

    private record PartitionResult(int index, short errorCode) {}
    private record TopicResult(String name, List<PartitionResult> partitions) {}

    private TopicResult readTopic(ProtocolReader reader, String groupId, short fencingError) {
        String topicName = reader.readString();
        List<PartitionResult> partitions = reader.readArray(r -> readPartition(r, groupId, topicName, fencingError));
        return new TopicResult(topicName, partitions);
    }

    private PartitionResult readPartition(ProtocolReader reader, String groupId, String topicName, short fencingError) {
        int partitionIndex = reader.readInt32();
        long committedOffset = reader.readInt64();
        reader.readInt64(); // commit_timestamp — deprecated even in real Kafka by this point; never read back
        String metadata = reader.readNullableString();

        if (fencingError != Errors.NONE) {
            return new PartitionResult(partitionIndex, fencingError);
        }
        offsetStore.commit(groupId, topicName, partitionIndex, committedOffset, metadata);
        return new PartitionResult(partitionIndex, Errors.NONE);
    }

    private static void writeTopicResult(ProtocolWriter w, TopicResult result) {
        w.writeString(result.name());
        w.writeArray(result.partitions(), (pw, p) -> {
            pw.writeInt32(p.index());
            pw.writeInt16(p.errorCode());
        });
    }
}
