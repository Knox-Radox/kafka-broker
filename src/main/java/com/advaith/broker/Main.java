package com.advaith.broker;

import com.advaith.broker.api.ApiHandler;
import com.advaith.broker.api.ApiVersionsHandler;
import com.advaith.broker.api.FetchHandler;
import com.advaith.broker.api.ListOffsetsHandler;
import com.advaith.broker.api.MetadataHandler;
import com.advaith.broker.api.ProduceHandler;
import com.advaith.broker.api.RequestDispatcher;
import com.advaith.broker.log.LogManager;
import com.advaith.broker.log.StorageConfig;
import com.advaith.broker.network.NetworkServer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Entry point: loads config, builds the topic-partition log from the
 * pre-configured topic list (PRD §5.1 — no auto-creation), wires all five
 * M1 API handlers, and starts the network layer on top of them.
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

        LogManager logManager = new LogManager(parseTopics(config.getProperty("topics", "")), dataDir, storageConfig);

        List<ApiHandler> handlers = List.of(
                new ApiVersionsHandler(),
                new MetadataHandler(brokerConfig, logManager),
                new ProduceHandler(logManager),
                new FetchHandler(logManager),
                new ListOffsetsHandler(logManager)
        );
        RequestDispatcher dispatcher = new RequestDispatcher(handlers);

        NetworkServer server = new NetworkServer(listenPort, dispatcher);
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
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
}
