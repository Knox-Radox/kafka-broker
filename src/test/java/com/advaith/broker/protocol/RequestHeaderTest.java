package com.advaith.broker.protocol;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class RequestHeaderTest {

    @Test
    void parsesV1HeaderWithClientIdAndNoTagBuffer() {
        // Metadata v4: header v1 per HeaderVersions -> client_id present, no tag buffer.
        ProtocolWriter writer = new ProtocolWriter();
        writer.writeInt16((short) ApiKey.METADATA.key);
        writer.writeInt16((short) 4);
        writer.writeInt32(777);
        writer.writeNullableString("test-client");

        RequestHeader header = RequestHeader.parse(new ProtocolReader(writer.toByteArray()));
        assertEquals(ApiKey.METADATA.key, header.apiKey());
        assertEquals(4, header.apiVersion());
        assertEquals(777, header.correlationId());
        assertEquals("test-client", header.clientId());
    }

    @Test
    void parsesV2HeaderWithClientIdAndTagBuffer() {
        // ApiVersions v3: header v2 -> client_id AND a tag buffer follow.
        ProtocolWriter writer = new ProtocolWriter();
        writer.writeInt16((short) ApiKey.API_VERSIONS.key);
        writer.writeInt16((short) 3);
        writer.writeInt32(42);
        writer.writeNullableString("test-client");
        writer.writeEmptyTagBuffer();
        writer.writeInt32(999); // a body field, to prove the tag buffer was correctly consumed

        ProtocolReader reader = new ProtocolReader(writer.toByteArray());
        RequestHeader header = RequestHeader.parse(reader);
        assertEquals(ApiKey.API_VERSIONS.key, header.apiKey());
        assertEquals(3, header.apiVersion());
        assertEquals(42, header.correlationId());
        assertEquals("test-client", header.clientId());
        assertEquals(999, reader.readInt32(), "body should start immediately after the tag buffer");
    }

    @Test
    void nullClientIdRoundTrips() {
        ProtocolWriter writer = new ProtocolWriter();
        writer.writeInt16((short) ApiKey.PRODUCE.key);
        writer.writeInt16((short) 3);
        writer.writeInt32(1);
        writer.writeNullableString(null);

        RequestHeader header = RequestHeader.parse(new ProtocolReader(writer.toByteArray()));
        assertNull(header.clientId());
    }

    @Test
    void responseHeaderWritesTagBufferOnlyAtV1() {
        ResponseHeader header = new ResponseHeader(55);

        ProtocolWriter v0 = new ProtocolWriter();
        header.write(v0, (short) 0);
        assertEquals(4, v0.toByteArray().length, "v0 is just the correlation id, 4 bytes");

        ProtocolWriter v1 = new ProtocolWriter();
        header.write(v1, (short) 1);
        assertEquals(5, v1.toByteArray().length, "v1 adds a 1-byte empty tag buffer");
    }
}
