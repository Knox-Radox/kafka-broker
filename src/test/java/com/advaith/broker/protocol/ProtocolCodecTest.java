package com.advaith.broker.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Round-trip tests for every primitive, at the boundary values PRD §5.7
 * specifically calls out: varint byte-count transitions (127/128,
 * 16383/16384) and zigzag's sign handling, plus the null cases for compact
 * types.
 */
class ProtocolCodecTest {

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 127, 128, 129, 16383, 16384, 16385, Integer.MAX_VALUE})
    void unsignedVarintRoundTrips(int value) {
        ProtocolWriter writer = new ProtocolWriter();
        writer.writeUnsignedVarint(value);
        ProtocolReader reader = new ProtocolReader(writer.toByteArray());
        assertEquals(value, reader.readUnsignedVarint());
    }

    @Test
    void unsignedVarintUsesExpectedByteCountAtBoundaries() {
        // 127 = 0111_1111 fits in 7 bits -> 1 byte, no continuation bit.
        assertEquals(1, encodeUnsigned(127).length);
        // 128 = 1000_0000 needs an 8th bit -> spills into a 2nd byte.
        assertEquals(2, encodeUnsigned(128).length);
        // 16383 = 2^14 - 1 fits in 14 bits -> 2 bytes.
        assertEquals(2, encodeUnsigned(16383).length);
        // 16384 = 2^14 needs a 15th bit -> spills into a 3rd byte.
        assertEquals(3, encodeUnsigned(16384).length);
    }

    @Test
    void twoHundredAndSevenEncodesToTheHandWorkedExample() {
        // 300 = 0b1_0010_1100. Split into 7-bit groups, low group first:
        // low 7 bits  = 010_1100 = 0x2C, with continuation bit set -> 0xAC
        // high bits   = 10       = 0x02, last byte, no continuation -> 0x02
        // i.e. the wire bytes are [0xAC, 0x02] — this is gate question 5.
        byte[] encoded = encodeUnsigned(300);
        assertArrayEquals(new byte[] {(byte) 0xAC, 0x02}, encoded);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1, 1, -2, 2, 63, -64, 64, Integer.MIN_VALUE, Integer.MAX_VALUE})
    void varintRoundTripsIncludingNegatives(int value) {
        ProtocolWriter writer = new ProtocolWriter();
        writer.writeVarint(value);
        ProtocolReader reader = new ProtocolReader(writer.toByteArray());
        assertEquals(value, reader.readVarint());
    }

    @Test
    void zigzagPacksSmallNegativesIntoOneByte() {
        // The whole point of zigzag: -1 must NOT cost 5 bytes (as raw two's
        // complement would, since it's all 1-bits), it should cost 1.
        assertEquals(1, encodeVarint(-1).length);
        assertEquals(1, encodeVarint(1).length);
    }

    @ParameterizedTest
    @ValueSource(longs = {0L, -1L, 1L, Long.MIN_VALUE, Long.MAX_VALUE})
    void varlongRoundTrips(long value) {
        ProtocolWriter writer = new ProtocolWriter();
        writer.writeVarlong(value);
        ProtocolReader reader = new ProtocolReader(writer.toByteArray());
        assertEquals(value, reader.readVarlong());
    }

    @Test
    void nullableStringRoundTripsIncludingNull() {
        assertEquals("hello", roundTripNullableString("hello"));
        assertEquals("", roundTripNullableString(""));
        assertNull(roundTripNullableString(null));
    }

    @Test
    void compactStringRoundTripsIncludingNull() {
        assertEquals("hello", roundTripCompactString("hello"));
        assertEquals("", roundTripCompactString(""));
        assertNull(roundTripCompactString(null));
    }

    @Test
    void nullableBytesRoundTripsIncludingNull() {
        byte[] data = {1, 2, 3};
        ProtocolWriter writer = new ProtocolWriter();
        writer.writeNullableBytes(data);
        writer.writeNullableBytes(null);
        ProtocolReader reader = new ProtocolReader(writer.toByteArray());
        assertArrayEquals(data, reader.readNullableBytes());
        assertNull(reader.readNullableBytes());
    }

    @Test
    void arrayRoundTripsIncludingNullAndEmpty() {
        ProtocolWriter writer = new ProtocolWriter();
        writer.<Integer>writeArray(List.of(1, 2, 3), (w, v) -> w.writeInt32(v));
        writer.<Integer>writeArray(List.of(), (w, v) -> w.writeInt32(v));
        writer.<Integer>writeArray(null, (w, v) -> w.writeInt32(v));

        ProtocolReader reader = new ProtocolReader(writer.toByteArray());
        assertEquals(List.of(1, 2, 3), reader.readArray(ProtocolReader::readInt32));
        assertEquals(List.of(), reader.readArray(ProtocolReader::readInt32));
        assertNull(reader.readArray(ProtocolReader::readInt32));
    }

    @Test
    void compactArrayRoundTripsIncludingNullAndEmpty() {
        ProtocolWriter writer = new ProtocolWriter();
        writer.<String>writeCompactArray(List.of("a", "b"), (w, v) -> w.writeCompactString(v));
        writer.<String>writeCompactArray(List.of(), (w, v) -> w.writeCompactString(v));
        writer.<String>writeCompactArray(null, (w, v) -> w.writeCompactString(v));

        ProtocolReader reader = new ProtocolReader(writer.toByteArray());
        assertEquals(List.of("a", "b"), reader.readCompactArray(ProtocolReader::readCompactString));
        assertEquals(List.of(), reader.readCompactArray(ProtocolReader::readCompactString));
        assertNull(reader.readCompactArray(ProtocolReader::readCompactString));
    }

    @Test
    void uuidRoundTrips() {
        UUID uuid = UUID.randomUUID();
        ProtocolWriter writer = new ProtocolWriter();
        writer.writeUuid(uuid);
        ProtocolReader reader = new ProtocolReader(writer.toByteArray());
        assertEquals(uuid, reader.readUuid());
    }

    @Test
    void tagBufferSkipsUnknownTagsWithoutError() {
        ProtocolWriter writer = new ProtocolWriter();
        writer.writeUnsignedVarint(2); // two tags we don't recognize
        writer.writeUnsignedVarint(7); // tag id 7
        writer.writeUnsignedVarint(3); // 3 bytes of data
        writer.writeRawBytes(new byte[] {9, 9, 9});
        writer.writeUnsignedVarint(12); // tag id 12
        writer.writeUnsignedVarint(0);  // zero-length data
        writer.writeInt32(42); // a real field that must still be readable after the skip

        ProtocolReader reader = new ProtocolReader(writer.toByteArray());
        reader.readTagBuffer();
        assertEquals(42, reader.readInt32());
    }

    @Test
    void growableWriterHandlesContentLargerThanInitialCapacity() {
        ProtocolWriter writer = new ProtocolWriter(4); // deliberately tiny, forces regrowth
        byte[] big = new byte[1000];
        for (int i = 0; i < big.length; i++) big[i] = (byte) i;
        writer.writeRawBytes(big);
        assertArrayEquals(big, writer.toByteArray());
    }

    private static byte[] encodeUnsigned(int value) {
        ProtocolWriter writer = new ProtocolWriter();
        writer.writeUnsignedVarint(value);
        return writer.toByteArray();
    }

    private static byte[] encodeVarint(int value) {
        ProtocolWriter writer = new ProtocolWriter();
        writer.writeVarint(value);
        return writer.toByteArray();
    }

    private static String roundTripNullableString(String value) {
        ProtocolWriter writer = new ProtocolWriter();
        writer.writeNullableString(value);
        return new ProtocolReader(writer.toByteArray()).readNullableString();
    }

    private static String roundTripCompactString(String value) {
        ProtocolWriter writer = new ProtocolWriter();
        writer.writeCompactNullableString(value);
        return new ProtocolReader(writer.toByteArray()).readCompactNullableString();
    }
}
