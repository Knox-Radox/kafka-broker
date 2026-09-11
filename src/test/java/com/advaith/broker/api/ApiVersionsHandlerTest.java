package com.advaith.broker.api;

import com.advaith.broker.protocol.ApiKey;
import com.advaith.broker.protocol.Errors;
import com.advaith.broker.protocol.ProtocolReader;
import com.advaith.broker.protocol.ProtocolWriter;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ApiVersionsHandlerTest {

    private final ApiVersionsHandler handler = new ApiVersionsHandler();

    @Test
    void supportedVersionAdvertisesAllFiveApisWithNoError() {
        ProtocolWriter requestBody = new ProtocolWriter();
        requestBody.writeCompactString("test-client");
        requestBody.writeCompactString("1.0");
        requestBody.writeEmptyTagBuffer();

        byte[] responseBytes = handler.handle((short) 3, new ProtocolReader(requestBody.toByteArray()));
        ProtocolReader response = new ProtocolReader(responseBytes);

        assertEquals(Errors.NONE, response.readInt16());
        List<int[]> apiKeys = response.readCompactArray(r -> {
            int[] entry = {r.readInt16(), r.readInt16(), r.readInt16()};
            r.readTagBuffer(); // each api_keys element carries its own TAG_BUFFER — must consume it
            return entry;
        });

        assertEquals(ApiKey.values().length, apiKeys.size());
        assertTrue(apiKeys.stream().anyMatch(k -> k[0] == ApiKey.API_VERSIONS.key
                && k[1] == ApiKey.API_VERSIONS.minVersion && k[2] == ApiKey.API_VERSIONS.maxVersion));

        assertEquals(0, response.readInt32(), "throttle_time_ms");
        response.readTagBuffer(); // top-level TAG_BUFFER
        assertEquals(0, response.remaining());
    }

    @Test
    void unsupportedVersionRepliesWithV0ShapeAndErrorThirtyFive() {
        // The handler must not even attempt to read a request body in this
        // path — an unsupported version might not be shaped like v3 at all.
        byte[] responseBytes = handler.handle((short) 99, new ProtocolReader(new byte[0]));
        ProtocolReader response = new ProtocolReader(responseBytes);

        assertEquals(Errors.UNSUPPORTED_VERSION, response.readInt16());
        List<Object> apiKeys = response.readArray(r -> null); // plain ARRAY, not compact
        assertEquals(0, apiKeys.size());
        assertEquals(0, response.remaining(), "v0 body has no throttle_time_ms and no tag buffer");
    }
}
