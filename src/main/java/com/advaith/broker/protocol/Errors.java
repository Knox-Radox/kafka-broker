package com.advaith.broker.protocol;

/**
 * Named constants for the handful of Kafka error codes M1 needs to emit.
 * Kafka defines ~90 of these; we only implement the ones our five APIs can
 * actually produce (PRD §5.3/§5.6), and name them so a response builder
 * never has a bare magic number in it.
 */
public final class Errors {
    private Errors() {}

    public static final short NONE = 0;
    public static final short OFFSET_OUT_OF_RANGE = 1;
    public static final short CORRUPT_MESSAGE = 2;
    public static final short UNKNOWN_TOPIC_OR_PARTITION = 3;
    public static final short INVALID_REQUEST = 42;
    public static final short UNSUPPORTED_VERSION = 35;
    public static final short INVALID_RECORD = 87;

    // M3 consumer groups (PRD §7.4/§7.5)
    public static final short ILLEGAL_GENERATION = 22;
    public static final short INCONSISTENT_GROUP_PROTOCOL = 23;
    public static final short UNKNOWN_MEMBER_ID = 25;
    public static final short INVALID_SESSION_TIMEOUT = 26;
    public static final short REBALANCE_IN_PROGRESS = 27;

    // M4 replication (PRD §8.5/§8.6): a request landed on a broker that
    // isn't (or no longer is) the partition's leader — the client's fix is
    // to refresh Metadata and retry against whoever the leader actually is
    // now, not to retry the same broker.
    public static final short NOT_LEADER_OR_FOLLOWER = 6;
    // acks=-1 parked waiting for the high-water mark to catch up to what
    // was just appended (PRD §8.6), and it never did within the request's
    // own timeout_ms — e.g. the ISR shrank to just the leader and stayed
    // there. The data IS on the leader's local log (the append itself
    // already succeeded); what didn't happen in time is enough replicas
    // confirming it, which is exactly what this real Kafka error code
    // names (as opposed to REQUEST_TIMED_OUT, which would wrongly imply
    // nothing happened at all).
    public static final short NOT_ENOUGH_REPLICAS_AFTER_APPEND = 20;
}
