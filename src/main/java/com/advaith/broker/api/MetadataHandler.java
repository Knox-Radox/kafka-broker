package com.advaith.broker.api;

import com.advaith.broker.BrokerConfig;
import com.advaith.broker.log.LogManager;
import com.advaith.broker.protocol.ApiKey;
import com.advaith.broker.protocol.Errors;
import com.advaith.broker.protocol.ProtocolReader;
import com.advaith.broker.protocol.ProtocolWriter;

import java.util.List;

/**
 * Lets a client discover topics, partitions, and which broker leads each
 * one (PRD §5.3). Design decision: the host/port advertised here — NOT the
 * address the client originally dialled — is where the client connects
 * next. M1 has exactly one broker, so it is trivially the leader of every
 * partition, but getting the advertised address wrong here is the classic
 * first bug (see JOURNAL.md's template entry): the client hangs silently
 * because it dials an address that accepted the Metadata request but isn't
 * where the broker actually told it the leader lives.
 */
public final class MetadataHandler implements ApiHandler {

    private final BrokerConfig config;
    private final LogManager logManager;

    public MetadataHandler(BrokerConfig config, LogManager logManager) {
        this.config = config;
        this.logManager = logManager;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.METADATA;
    }

    @Override
    public byte[] handle(short apiVersion, ProtocolReader request) {
        List<String> requestedTopics = request.readArray(ProtocolReader::readString); // null = "all topics"
        request.readBoolean(); // allow_auto_topic_creation — ignored, M1 never auto-creates (PRD §5.1)

        List<String> topicsToDescribe = requestedTopics != null ? requestedTopics : List.copyOf(logManager.topicNames());

        ProtocolWriter response = new ProtocolWriter();
        response.writeInt32(0); // throttle_time_ms

        response.writeArray(List.of(config), (w, ignored) -> {
            w.writeInt32(config.brokerId());
            w.writeString(config.advertisedHost());
            w.writeInt32(config.advertisedPort());
            w.writeNullableString(null); // rack — not modeled in M1
        });

        response.writeNullableString(config.clusterId());
        response.writeInt32(config.brokerId()); // controller_id — the one broker is trivially its own controller

        response.writeArray(topicsToDescribe, (w, topicName) -> writeTopic(w, topicName));
        return response.toByteArray();
    }

    private void writeTopic(ProtocolWriter w, String topicName) {
        if (!logManager.topicExists(topicName)) {
            w.writeInt16(Errors.UNKNOWN_TOPIC_OR_PARTITION);
            w.writeString(topicName);
            w.writeBoolean(false); // is_internal
            w.writeArray(List.of(), (pw, ignored) -> {}); // no partitions to report for a topic that doesn't exist
            return;
        }

        w.writeInt16(Errors.NONE);
        w.writeString(topicName);
        w.writeBoolean(false); // is_internal — M1 defines no internal topics (e.g. no __consumer_offsets yet; that's M3)

        int partitionCount = logManager.partitionCount(topicName);
        List<Integer> partitionIndices = java.util.stream.IntStream.range(0, partitionCount).boxed().toList();
        w.writeArray(partitionIndices, (pw, index) -> {
            pw.writeInt16(Errors.NONE);
            pw.writeInt32(index);
            pw.writeInt32(config.brokerId()); // leader_id — the only broker, always the leader (M4 changes this)
            pw.writeArray(List.of(config.brokerId()), ProtocolWriter::writeInt32); // replica_nodes
            pw.writeArray(List.of(config.brokerId()), ProtocolWriter::writeInt32); // isr_nodes — trivially in sync with itself
        });
    }
}
