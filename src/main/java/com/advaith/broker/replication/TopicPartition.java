package com.advaith.broker.replication;

/**
 * A (topic, partition) pair used as a map key across the replication
 * package. Nothing above M3 needed this as its own type — every handler
 * just carried the two values as separate parameters — but M4's
 * ReplicaManager tracks per-partition state in maps keyed by both at once
 * often enough that a record earns its place here.
 */
public record TopicPartition(String topic, int partition) {}
