package com.advaith.broker.protocol;

import java.util.Optional;

/**
 * The five APIs M1 implements, and the exact version we support for each
 * (PRD §5.1/§5.3 — min == max everywhere, deliberately: we advertise a
 * single version per API in ApiVersions so a spec-compliant client
 * negotiates straight down to it instead of guessing).
 */
public enum ApiKey {
    PRODUCE(0, (short) 3, (short) 3, "Produce"),
    FETCH(1, (short) 4, (short) 4, "Fetch"),
    LIST_OFFSETS(2, (short) 1, (short) 1, "ListOffsets"),
    METADATA(3, (short) 4, (short) 4, "Metadata"),
    // M3 consumer groups (PRD §7.4). Every version chosen here is, like the
    // five M1 APIs above, deliberately the oldest non-flexible version that
    // carries the fields we need — verified against the real, versioned
    // JSON schemas in the Apache Kafka source (clients/src/main/resources/
    // common/message/*.json), not recalled from memory, specifically
    // because getting one of these seven subtly wrong (a field order, a
    // STRING where it should be NULLABLE_STRING) is exactly the kind of
    // mistake M1's ListOffsets throttle_time_ms bug already taught us to
    // check for rather than assume (see JOURNAL.md, 2026-09-11).
    OFFSET_COMMIT(8, (short) 1, (short) 1, "OffsetCommit"),
    OFFSET_FETCH(9, (short) 1, (short) 1, "OffsetFetch"),
    FIND_COORDINATOR(10, (short) 0, (short) 0, "FindCoordinator"),
    JOIN_GROUP(11, (short) 1, (short) 1, "JoinGroup"),
    HEARTBEAT(12, (short) 0, (short) 0, "Heartbeat"),
    LEAVE_GROUP(13, (short) 0, (short) 0, "LeaveGroup"),
    SYNC_GROUP(14, (short) 0, (short) 0, "SyncGroup"),
    // ApiVersions alone advertises min=0: it's the API a client uses to
    // discover every other API's version range, including its own, so it
    // cannot rely on that same negotiation to recover if its first guess is
    // wrong. Every real client's fallback on a failed guess is to retry at
    // v0 specifically — the one version guaranteed old enough for any
    // broker to understand — so a broker that doesn't keep answering v0
    // correctly forever cannot complete a handshake with a client that
    // guessed high on its first try (see JOURNAL.md, 2026-09-11).
    API_VERSIONS(18, (short) 0, (short) 3, "ApiVersions");

    public final int key;
    public final short minVersion;
    public final short maxVersion;
    /**
     * Real Kafka's own API display name (as it appears in Kafka's JMX
     * request-metric MBean names) — introduced for M5 (PRD §9.4) so
     * {@code broker_requests_total{api="Produce"}} mirrors that same
     * convention translated to Prometheus's label style, rather than
     * inventing a different naming scheme from the enum constant's own
     * SCREAMING_SNAKE_CASE.
     */
    public final String displayName;

    ApiKey(int key, short minVersion, short maxVersion, String displayName) {
        this.key = key;
        this.minVersion = minVersion;
        this.maxVersion = maxVersion;
        this.displayName = displayName;
    }

    public boolean supportsVersion(short version) {
        return version >= minVersion && version <= maxVersion;
    }

    public static Optional<ApiKey> forKey(int key) {
        for (ApiKey apiKey : values()) {
            if (apiKey.key == key) {
                return Optional.of(apiKey);
            }
        }
        return Optional.empty();
    }
}
