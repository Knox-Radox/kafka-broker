package com.advaith.broker.protocol;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The table PRD §5.2 calls "the trap that breaks most implementations" —
 * specifically that ApiVersions v3's response header is v0 even though its
 * request header is the flexible v2.
 */
class HeaderVersionsTest {

    @Test
    void apiVersionsV3UsesFlexibleRequestHeaderButV0ResponseHeader() {
        HeaderVersions.Versions versions = HeaderVersions.lookup(ApiKey.API_VERSIONS.key, (short) 3);
        assertEquals((short) 2, versions.requestHeaderVersion(), "v3 request header should be flexible (v2)");
        assertEquals((short) 0, versions.responseHeaderVersion(), "v3 response header must stay v0 regardless");
    }

    @Test
    void apiVersionsResponseHeaderIsAlwaysV0RegardlessOfRequestedVersion() {
        for (short version = 0; version <= 3; version++) {
            assertEquals((short) 0, HeaderVersions.lookup(ApiKey.API_VERSIONS.key, version).responseHeaderVersion(),
                    "ApiVersions response header must be v0 at version " + version);
        }
    }

    @Test
    void apiVersionsRequestHeaderVersionRisesWithApiVersion() {
        assertEquals((short) 0, HeaderVersions.lookup(ApiKey.API_VERSIONS.key, (short) 0).requestHeaderVersion());
        assertEquals((short) 1, HeaderVersions.lookup(ApiKey.API_VERSIONS.key, (short) 1).requestHeaderVersion());
        assertEquals((short) 1, HeaderVersions.lookup(ApiKey.API_VERSIONS.key, (short) 2).requestHeaderVersion());
        assertEquals((short) 2, HeaderVersions.lookup(ApiKey.API_VERSIONS.key, (short) 3).requestHeaderVersion());
    }

    @Test
    void theOtherFourM1ApisUseNonFlexibleHeadersAtTheVersionsWeImplement() {
        assertHeaders(ApiKey.PRODUCE, (short) 3);
        assertHeaders(ApiKey.FETCH, (short) 4);
        assertHeaders(ApiKey.LIST_OFFSETS, (short) 1);
        assertHeaders(ApiKey.METADATA, (short) 4);
    }

    private static void assertHeaders(ApiKey apiKey, short version) {
        HeaderVersions.Versions versions = HeaderVersions.lookup(apiKey.key, version);
        assertEquals((short) 1, versions.requestHeaderVersion(), apiKey + " request header");
        assertEquals((short) 0, versions.responseHeaderVersion(), apiKey + " response header");
    }
}
