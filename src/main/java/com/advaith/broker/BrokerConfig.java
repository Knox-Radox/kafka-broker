package com.advaith.broker;

/**
 * The broker's own identity, as opposed to LogManager's knowledge of
 * topics. Exists as its own small type mainly because of advertisedHost:
 * MetadataHandler must hand this out to clients verbatim, and it is
 * deliberately NOT the same value as the address this process binds to
 * (see the advertised-vs-bind distinction in config/broker.properties and
 * JOURNAL.md's template entry).
 */
public record BrokerConfig(int brokerId, String advertisedHost, int advertisedPort, String clusterId) {}
