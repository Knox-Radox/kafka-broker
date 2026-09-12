package com.advaith.broker.api;

import com.advaith.broker.BrokerConfig;
import com.advaith.broker.group.GroupCoordinator;
import com.advaith.broker.log.LogManager;
import com.advaith.broker.protocol.ApiKey;
import com.advaith.broker.protocol.Errors;
import com.advaith.broker.protocol.ProtocolReader;
import com.advaith.broker.protocol.ProtocolWriter;
import com.advaith.broker.replication.PeerInfo;
import com.advaith.broker.replication.ReplicaManager;
import com.advaith.broker.replication.ReplicationConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * Lets a client discover topics, partitions, and which broker leads each
 * one (PRD §5.3). Design decision: the host/port advertised here — NOT the
 * address the client originally dialled — is where the client connects
 * next. M1-M3 had exactly one broker, so it was trivially the leader of
 * every partition; getting the advertised address wrong here is the
 * classic first bug (see JOURNAL.md's template entry): the client hangs
 * silently because it dials an address that accepted the Metadata request
 * but isn't where the broker actually told it the leader lives.
 *
 * Since M4 (PRD §8), that address can point at a DIFFERENT broker than
 * the one answering this Metadata request — this handler now reports
 * every broker in the cluster (self + configured peers), and every
 * partition's leader/replicas/ISR come from {@link ReplicaManager}'s live
 * belief instead of being hardcoded to "always me".
 */
public final class MetadataHandler implements ApiHandler {

    private final BrokerConfig config;
    private final LogManager logManager;
    private final ReplicaManager replicaManager;
    private final List<PeerInfo> peers;

    public MetadataHandler(BrokerConfig config, LogManager logManager) {
        // Single-broker default (see ReplicaManager's javadoc): every
        // partition trivially self-led, no other brokers to report.
        this(config, logManager, new ReplicaManager(config.brokerId(), logManager, ReplicationConfig.singleBrokerDefault()), List.of());
    }

    public MetadataHandler(BrokerConfig config, LogManager logManager, ReplicaManager replicaManager, List<PeerInfo> peers) {
        this.config = config;
        this.logManager = logManager;
        this.replicaManager = replicaManager;
        this.peers = peers;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.METADATA;
    }

    @Override
    public byte[] handle(RequestContext context, ProtocolReader request) {
        List<String> requestedTopics = request.readArray(ProtocolReader::readString); // null = "all topics"
        request.readBoolean(); // allow_auto_topic_creation — ignored, M1 never auto-creates (PRD §5.1)

        // "all topics" must not leak the internal offsets topic (PRD §7.4)
        // — real Kafka excludes __consumer_offsets from an unscoped listing
        // too, though (like real Kafka) explicitly naming it still resolves
        // it normally below, since nothing about it needs hiding from a
        // client that already knows what it's asking for.
        List<String> topicsToDescribe = requestedTopics != null
                ? requestedTopics
                : logManager.topicNames().stream().filter(name -> !name.equals(GroupCoordinator.OFFSETS_TOPIC)).toList();

        ProtocolWriter response = new ProtocolWriter();
        response.writeInt32(0); // throttle_time_ms

        List<Object[]> brokers = new ArrayList<>(); // {brokerId, host, port} — plain arrays, this list never leaves this method
        brokers.add(new Object[] {config.brokerId(), config.advertisedHost(), config.advertisedPort()});
        for (PeerInfo peer : peers) {
            brokers.add(new Object[] {peer.brokerId(), peer.host(), peer.port()});
        }
        response.writeArray(brokers, (w, b) -> {
            w.writeInt32((int) b[0]);
            w.writeString((String) b[1]);
            w.writeInt32((int) b[2]);
            w.writeNullableString(null); // rack — not modeled in this project
        });

        response.writeNullableString(config.clusterId());
        // controller_id: this project has no real controller (PRD §2 —
        // KRaft-compatible metadata quorum is explicitly out of scope;
        // §8.1 chose lease-based failover specifically instead of one) —
        // every broker just reports itself, which is harmless because
        // nothing here implements an admin API that would ever actually
        // dial "the controller" using this field.
        response.writeInt32(config.brokerId());

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
        w.writeBoolean(topicName.equals(GroupCoordinator.OFFSETS_TOPIC)); // is_internal (PRD §7.4)

        int partitionCount = logManager.partitionCount(topicName);
        List<Integer> partitionIndices = java.util.stream.IntStream.range(0, partitionCount).boxed().toList();
        w.writeArray(partitionIndices, (pw, index) -> {
            pw.writeInt16(Errors.NONE);
            pw.writeInt32(index);
            pw.writeInt32(replicaManager.leaderIdFor(topicName, index)); // PRD §8.5: this broker's own current belief, not necessarily itself anymore
            pw.writeArray(replicaManager.replicasFor(topicName, index), ProtocolWriter::writeInt32);
            pw.writeArray(replicaManager.isrFor(topicName, index), ProtocolWriter::writeInt32);
        });
    }
}
