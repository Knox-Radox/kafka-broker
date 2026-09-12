package com.advaith.broker.group;

/**
 * One (name, opaque metadata bytes) entry from a JoinGroupRequest's
 * {@code group_protocols} array — the client's own subscription/assignor
 * info, per PRD §7.1: "an opaque byte blob per real Kafka's protocol — the
 * broker doesn't need to understand its contents." Public and top-level
 * (not nested in the package-private {@code ConsumerGroup}) because
 * {@code JoinGroupHandler}, in the api package, has to build a list of
 * these straight from the wire to hand to {@link GroupCoordinator}.
 */
public record ProtocolMetadata(String name, byte[] metadata) {}
