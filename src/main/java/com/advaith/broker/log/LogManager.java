package com.advaith.broker.log;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Owns every topic-partition's PartitionLog. Design decision: topics and
 * their partition counts are fixed once at construction, from startup
 * config — M1 has no topic auto-creation (PRD §5.1), so there is no code
 * path that adds a topic after the broker has started. Since M2, each
 * partition also gets its own directory under dataDir, named
 * "<topic>-<partition>" — the same naming real Kafka uses on disk.
 */
public final class LogManager {

    private final Map<String, PartitionLog[]> partitionsByTopic = new HashMap<>();

    public LogManager(Map<String, Integer> partitionCountsByTopic, Path dataDir, StorageConfig storageConfig) {
        for (var entry : partitionCountsByTopic.entrySet()) {
            String topic = entry.getKey();
            PartitionLog[] partitions = new PartitionLog[entry.getValue()];
            for (int i = 0; i < partitions.length; i++) {
                Path partitionDir = dataDir.resolve(topic + "-" + i);
                partitions[i] = new PartitionLog(partitionDir, storageConfig);
            }
            partitionsByTopic.put(topic, partitions);
        }
    }

    public boolean topicExists(String topic) {
        return partitionsByTopic.containsKey(topic);
    }

    public int partitionCount(String topic) {
        PartitionLog[] partitions = partitionsByTopic.get(topic);
        return partitions == null ? 0 : partitions.length;
    }

    public Optional<PartitionLog> getPartition(String topic, int partition) {
        PartitionLog[] partitions = partitionsByTopic.get(topic);
        if (partitions == null || partition < 0 || partition >= partitions.length) {
            return Optional.empty();
        }
        return Optional.of(partitions[partition]);
    }

    public Set<String> topicNames() {
        return partitionsByTopic.keySet();
    }

    /** Flushes and closes every partition's file handles — graceful shutdown, and how a test simulates a restart by opening a fresh LogManager against the same dataDir afterward (PartitionLog.close() carries the same caveat: a real kill -9 never calls this at all). */
    public void close() {
        for (PartitionLog[] partitions : partitionsByTopic.values()) {
            for (PartitionLog partition : partitions) {
                partition.close();
            }
        }
    }
}
