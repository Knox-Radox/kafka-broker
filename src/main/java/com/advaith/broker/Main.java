package com.advaith.broker;

import com.advaith.broker.api.ApiHandler;
import com.advaith.broker.api.ApiVersionsHandler;
import com.advaith.broker.api.FetchHandler;
import com.advaith.broker.api.FindCoordinatorHandler;
import com.advaith.broker.api.HeartbeatHandler;
import com.advaith.broker.api.JoinGroupHandler;
import com.advaith.broker.api.LeaveGroupHandler;
import com.advaith.broker.api.ListOffsetsHandler;
import com.advaith.broker.api.MetadataHandler;
import com.advaith.broker.api.OffsetCommitHandler;
import com.advaith.broker.api.OffsetFetchHandler;
import com.advaith.broker.api.ProduceHandler;
import com.advaith.broker.api.RequestDispatcher;
import com.advaith.broker.api.SyncGroupHandler;
import com.advaith.broker.group.GroupConfig;
import com.advaith.broker.group.GroupCoordinator;
import com.advaith.broker.group.OffsetStore;
import com.advaith.broker.log.LogManager;
import com.advaith.broker.log.StorageConfig;
import com.advaith.broker.metrics.Metrics;
import com.advaith.broker.metrics.MetricsExporter;
import com.advaith.broker.metrics.MetricsServer;
import com.advaith.broker.network.NetworkServer;
import com.advaith.broker.network.SelectorTicker;
import com.advaith.broker.replication.PeerInfo;
import com.advaith.broker.replication.ReplicaManager;
import com.advaith.broker.replication.ReplicationConfig;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Entry point: loads config, builds the topic-partition log from the
 * pre-configured topic list (PRD §5.1 — no auto-creation) plus the
 * internal consumer-offsets topic (§7.4), wires every API handler through
 * M3, and starts the network layer on top of them.
 */
public final class Main {

    public static void main(String[] args) throws IOException {
        String configPath = args.length > 0 ? args[0] : "config/broker.properties";
        Properties config = new Properties();
        try (InputStream in = Files.newInputStream(Path.of(configPath))) {
            config.load(in);
        }

        int listenPort = Integer.parseInt(config.getProperty("listen.port", "9092"));
        BrokerConfig brokerConfig = new BrokerConfig(
                Integer.parseInt(config.getProperty("broker.id", "0")),
                config.getProperty("advertised.host", "127.0.0.1"),
                Integer.parseInt(config.getProperty("advertised.port", String.valueOf(listenPort))),
                "kafka-broker-m1-cluster");

        StorageConfig storageConfig = new StorageConfig(
                Long.parseLong(config.getProperty("log.segment.bytes", "1048576")),
                Integer.parseInt(config.getProperty("log.index.interval.bytes", "4096")),
                Long.parseLong(config.getProperty("log.retention.bytes", "-1")),
                Long.parseLong(config.getProperty("log.retention.ms", "-1")),
                Integer.parseInt(config.getProperty("log.flush.interval.messages", "1")),
                Long.parseLong(config.getProperty("log.flush.interval.ms", "-1")));
        Path dataDir = Path.of(config.getProperty("data.dir", "data"));

        int offsetsTopicPartitions = Integer.parseInt(config.getProperty("offsets.topic.num.partitions", "8"));
        Map<String, Integer> topics = parseTopics(config.getProperty("topics", ""));
        topics.put(GroupCoordinator.OFFSETS_TOPIC, offsetsTopicPartitions);

        LogManager logManager = new LogManager(topics, dataDir, storageConfig);
        OffsetStore offsetStore = new OffsetStore(logManager, offsetsTopicPartitions);

        List<PeerInfo> peers = parsePeers(config.getProperty("broker.peers", ""));
        ReplicationConfig replicationConfig = new ReplicationConfig(
                peers,
                Integer.parseInt(config.getProperty("replication.factor", "1")),
                Long.parseLong(config.getProperty("replica.lag.time.max.ms", "10000")),
                Long.parseLong(config.getProperty("leader.lease.renew.interval.ms", "2000")),
                Long.parseLong(config.getProperty("leader.lease.timeout.ms", "6000")),
                Integer.parseInt(config.getProperty("replica.fetch.max.wait.ms", "500")),
                Integer.parseInt(config.getProperty("replica.fetch.min.bytes", "1")));
        ReplicaManager replicaManager = new ReplicaManager(brokerConfig.brokerId(), logManager, replicationConfig);

        GroupConfig groupConfig = new GroupConfig(
                Integer.parseInt(config.getProperty("group.min.session.timeout.ms", "6000")),
                Integer.parseInt(config.getProperty("group.max.session.timeout.ms", "300000")),
                Integer.parseInt(config.getProperty("group.rebalance.timeout.ms", "60000")),
                Integer.parseInt(config.getProperty("group.initial.rebalance.delay.ms", "3000")));
        GroupCoordinator groupCoordinator = new GroupCoordinator(groupConfig);

        int fetchMaxWaitMsCap = Integer.parseInt(config.getProperty("fetch.max.wait.ms", "500"));
        FetchHandler fetchHandler = new FetchHandler(logManager, fetchMaxWaitMsCap, replicaManager);
        ProduceHandler produceHandler = new ProduceHandler(logManager, fetchHandler, replicaManager);
        // ProduceHandler needs to hear about every HWM movement (its own
        // leader-side appends AND every incoming replica Fetch that might
        // move the ISR minimum) to wake a parked acks=-1 request the
        // instant it's satisfied (PRD §8.6) instead of waiting for its own
        // next timeout sweep.
        replicaManager.setHwmAdvancedListener(produceHandler);

        List<ApiHandler> handlers = List.of(
                new ApiVersionsHandler(),
                new MetadataHandler(brokerConfig, logManager, replicaManager, peers),
                produceHandler,
                fetchHandler,
                new ListOffsetsHandler(logManager, replicaManager),
                new FindCoordinatorHandler(brokerConfig, offsetsTopicPartitions),
                new JoinGroupHandler(groupCoordinator),
                new SyncGroupHandler(groupCoordinator),
                new HeartbeatHandler(groupCoordinator),
                new LeaveGroupHandler(groupCoordinator),
                new OffsetCommitHandler(groupCoordinator, offsetStore),
                new OffsetFetchHandler(offsetStore)
        );
        Metrics metrics = new Metrics();
        RequestDispatcher dispatcher = new RequestDispatcher(handlers, metrics);

        // M5 (PRD §9.4): a real Prometheus text-exposition endpoint on its
        // own port, its own JDK HttpServer, its own thread pool — see
        // MetricsServer's javadoc for why it must never share
        // NetworkServer's single selector thread.
        int metricsPort = Integer.parseInt(config.getProperty("metrics.port", "9404"));
        MetricsExporter metricsExporter = new MetricsExporter(metrics, logManager, replicaManager);
        MetricsServer metricsServer = new MetricsServer(metricsPort, metricsExporter);

        // Three independent things need the selector loop to wake itself
        // up on a schedule, for the same underlying reason (§7.3's
        // javadoc) but driven by unrelated state: a parked Fetch's
        // timeout, a group's rebalance/heartbeat deadlines, and (since M4)
        // a parked acks=-1 produce's own timeout_ms. SelectorTicker.combine
        // lets NetworkServer keep holding just one reference. The replica
        // fetch loop itself is NOT one of these — it runs on its own
        // dedicated per-peer threads (see PeerReplicator's javadoc for why).
        NetworkServer server = new NetworkServer(listenPort, dispatcher, SelectorTicker.combine(fetchHandler, groupCoordinator, produceHandler));
        replicaManager.start();
        metricsServer.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.stop();
            replicaManager.stop();
            metricsServer.stop();
        }));
        server.run();
    }

    /** Parses "name:partitionCount,name2:partitionCount2" from config (PRD §5.5). */
    private static Map<String, Integer> parseTopics(String topicsProperty) {
        Map<String, Integer> topics = new HashMap<>();
        for (String entry : topicsProperty.split(",")) {
            String trimmed = entry.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            String[] parts = trimmed.split(":");
            topics.put(parts[0], Integer.parseInt(parts[1]));
        }
        return topics;
    }

    /** Parses "brokerId:host:port,brokerId2:host2:port2" (PRD §8.2/§8.7) — every OTHER broker in the cluster, never including this one. */
    private static List<PeerInfo> parsePeers(String peersProperty) {
        List<PeerInfo> peers = new ArrayList<>();
        for (String entry : peersProperty.split(",")) {
            String trimmed = entry.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            String[] parts = trimmed.split(":");
            peers.add(new PeerInfo(Integer.parseInt(parts[0]), parts[1], Integer.parseInt(parts[2])));
        }
        return peers;
    }
}
