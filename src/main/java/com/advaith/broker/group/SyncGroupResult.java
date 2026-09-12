package com.advaith.broker.group;

/** What {@link GroupCoordinator#syncGroup} eventually hands back to one member — its own slice of the assignment the leader computed. */
public record SyncGroupResult(short errorCode, byte[] assignment) {

    static SyncGroupResult error(short errorCode) {
        return new SyncGroupResult(errorCode, new byte[0]);
    }
}
