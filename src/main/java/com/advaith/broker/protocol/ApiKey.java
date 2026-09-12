package com.advaith.broker.protocol;

import java.util.Optional;

/**
 * The five APIs M1 implements, and the exact version we support for each
 * (PRD §5.1/§5.3 — min == max everywhere, deliberately: we advertise a
 * single version per API in ApiVersions so a spec-compliant client
 * negotiates straight down to it instead of guessing).
 */
public enum ApiKey {
    PRODUCE(0, (short) 3, (short) 3),
    FETCH(1, (short) 4, (short) 4),
    LIST_OFFSETS(2, (short) 1, (short) 1),
    METADATA(3, (short) 4, (short) 4),
    // M3 consumer groups (PRD §7.4). Every version chosen here is, like the
    // five M1 APIs above, deliberately the oldest non-flexible version that
    // carries the fields we need — verified against the real, versioned
    // JSON schemas in the Apache Kafka source (clients/src/main/resources/
    // common/message/*.json), not recalled from memory, specifically
    // because getting one of these seven subtly wrong (a field order, a
    // STRING where it should be NULLABLE_STRING) is exactly the kind of
    // mistake M1's ListOffsets throttle_time_ms bug already taught us to
    // check for rather than assume (see JOURNAL.md, 2026-09-11).
    OFFSET_COMMIT(8, (short) 1, (short) 1),
    OFFSET_FETCH(9, (short) 1, (short) 1),
    FIND_COORDINATOR(10, (short) 0, (short) 0),
    JOIN_GROUP(11, (short) 1, (short) 1),
    HEARTBEAT(12, (short) 0, (short) 0),
    LEAVE_GROUP(13, (short) 0, (short) 0),
    SYNC_GROUP(14, (short) 0, (short) 0),
    // ApiVersions alone advertises min=0: it's the API a client uses to
    // discover every other API's version range, including its own, so it
    // cannot rely on that same negotiation to recover if its first guess is
    // wrong. Every real client's fallback on a failed guess is to retry at
    // v0 specifically — the one version guaranteed old enough for any
    // broker to understand — so a broker that doesn't keep answering v0
    // correctly forever cannot complete a handshake with a client that
    // guessed high on its first try (see JOURNAL.md, 2026-09-11).
    API_VERSIONS(18, (short) 0, (short) 3);

    public final int key;
    public final short minVersion;
    public final short maxVersion;

    ApiKey(int key, short minVersion, short maxVersion) {
        this.key = key;
        this.minVersion = minVersion;
        this.maxVersion = maxVersion;
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
