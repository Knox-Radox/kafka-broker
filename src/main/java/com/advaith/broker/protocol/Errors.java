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
}
