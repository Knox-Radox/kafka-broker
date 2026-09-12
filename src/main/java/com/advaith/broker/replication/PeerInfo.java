package com.advaith.broker.replication;

/**
 * One entry from {@code broker.peers} (PRD §8.2/§8.7): another broker in
 * the cluster, and the address to dial to reach it. Deliberately as
 * minimal as BrokerConfig — the peer list is static config, not something
 * discovered, matching the project's established pattern (topics since
 * M1, group config since M3).
 */
public record PeerInfo(int brokerId, String host, int port) {}
