package com.advaith.broker.log;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Owns every topic-partition's PartitionLog. Design decision: topics and
 * their partition counts are fixed once at construction, from startup
 * config — M1 has no topic auto-creation (PRD §5.1), so there is no code
 * path that adds a topic after the broker has started.
 */
public final class LogManager {

    private final Map<String, PartitionLog[]> partitionsByTopic = new HashMap<>();

    public LogManager(Map<String, Integer> partitionCountsByTopic) {
        for (var entry : partitionCountsByTopic.entrySet()) {
            PartitionLog[] partitions = new PartitionLog[entry.getValue()];
            for (int i = 0; i < partitions.length; i++) {
                partitions[i] = new PartitionLog();
            }
            partitionsByTopic.put(entry.getKey(), partitions);
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
}
