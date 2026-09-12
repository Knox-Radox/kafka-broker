package com.advaith.broker.protocol;

/**
 * (api_key, api_version) → (request_header_version, response_header_version).
 * This is the "trap table" from PRD §5.2: header version is NOT a pure
 * function of "is this version flexible?" — ApiVersions is a permanent,
 * deliberate exception, and this class exists to make that exception a
 * lookup, not something re-derived (and re-gotten-wrong) at every call site.
 */
public final class HeaderVersions {

    public record Versions(short requestHeaderVersion, short responseHeaderVersion) {}

    private HeaderVersions() {}

    public static Versions lookup(int apiKey, short apiVersion) {
        if (apiKey == ApiKey.API_VERSIONS.key) {
            return apiVersionsHeaders(apiVersion);
        }

        // Every other API we implement (Produce v3, Fetch v4, ListOffsets
        // v1, Metadata v4, and — since M3 — OffsetCommit v1, OffsetFetch
        // v1, FindCoordinator v0, JoinGroup v1, Heartbeat v0, LeaveGroup
        // v0, SyncGroup v0) was chosen specifically because it sits below
        // that API's real flexible-version threshold in upstream Kafka
        // (thresholds range from v3 for FindCoordinator up to v12 for
        // Fetch — see ApiKey's own comment for where each version came
        // from). Below that threshold the rule is uniform: request header
        // v1 (client_id, no tag buffer), response header v0 (no tag
        // buffer). This is hard-coded rather than derived because we don't
        // implement — and therefore don't need to know the thresholds for —
        // any other version of these APIs; see ApiKey for the versions we
        // actually advertise.
        return new Versions((short) 1, (short) 0);
    }

    /**
     * ApiVersions is the client's very first request, sent before it knows
     * anything the broker supports — so the response header can't depend on
     * negotiation succeeding. Kafka's protocol permanently pins the
     * response header to v0 for every version of this API, precisely so a
     * client that guessed the wrong version can still parse the error back.
     * The request header, by contrast, follows the normal flexible-version
     * rule, because the *client* controls what it sends and knows its own
     * version.
     */
    private static Versions apiVersionsHeaders(short apiVersion) {
        short requestHeaderVersion;
        if (apiVersion >= 3) {
            requestHeaderVersion = 2; // flexible: client_id + TAG_BUFFER
        } else if (apiVersion >= 1) {
            requestHeaderVersion = 1; // client_id, no TAG_BUFFER
        } else {
            requestHeaderVersion = 0; // no client_id, no TAG_BUFFER
        }
        return new Versions(requestHeaderVersion, (short) 0);
    }
}
