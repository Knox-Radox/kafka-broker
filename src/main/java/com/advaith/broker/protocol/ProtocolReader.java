package com.advaith.broker.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

/**
 * Decodes Kafka primitive wire types from a ByteBuffer positioned at the
 * start of undecoded data. One method per primitive in PRD §5.2, so every
 * handler above this layer trusts one implementation of the fiddly parts
 * (varints, zigzag, compact length+1) instead of re-deriving them.
 */
public final class ProtocolReader {

    private final ByteBuffer buf;

    public ProtocolReader(ByteBuffer buf) {
        this.buf = buf;
    }

    public ProtocolReader(byte[] bytes) {
        this(ByteBuffer.wrap(bytes));
    }

    public int remaining() {
        return buf.remaining();
    }

    /** Current absolute offset into the underlying bytes — used where a caller needs to know exactly where a field started (e.g. CRC coverage boundaries). */
    public int position() {
        return buf.position();
    }

    // ---- fixed-width ----

    public byte readInt8() {
        return buf.get();
    }

    public short readInt16() {
        return buf.getShort();
    }

    public int readInt32() {
        return buf.getInt();
    }

    public long readInt64() {
        return buf.getLong();
    }

    public boolean readBoolean() {
        return readInt8() != 0;
    }

    // ---- varints ----

    /**
     * UNSIGNED_VARINT: 7 payload bits per byte, MSB = "more bytes follow".
     * Unlike everything else in this protocol, the byte groups are ordered
     * least-significant-first — a deliberate exception worth remembering,
     * not a mistake.
     */
    public int readUnsignedVarint() {
        int value = 0;
        int shift = 0;
        while (true) {
            byte b = readInt8();
            value |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return value;
            }
            shift += 7;
            if (shift > 28) {
                throw new IllegalArgumentException("varint is too long (more than 5 bytes)");
            }
        }
    }

    /** VARINT: zigzag-decoded signed value, carried as an UNSIGNED_VARINT. */
    public int readVarint() {
        int raw = readUnsignedVarint();
        // Zigzag decode: raw's low bit is the sign; the rest is the
        // magnitude shifted left by one. This inverse of the encode below
        // recovers ...,-2,-1,0,1,2,... from 4,2,0,1,3,....
        return (raw >>> 1) ^ -(raw & 1);
    }

    /** VARLONG: same idea as VARINT, at 64-bit width (used by timestampDelta). */
    public long readVarlong() {
        long value = 0;
        int shift = 0;
        while (true) {
            byte b = readInt8();
            value |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                break;
            }
            shift += 7;
            if (shift > 63) {
                throw new IllegalArgumentException("varlong is too long (more than 10 bytes)");
            }
        }
        return (value >>> 1) ^ -(value & 1);
    }

    // ---- strings ----

    /** STRING / NULLABLE_STRING: INT16 length, then UTF-8 bytes; -1 length = null. */
    public String readNullableString() {
        short length = readInt16();
        if (length < 0) {
            return null;
        }
        return new String(readRawBytes(length), StandardCharsets.UTF_8);
    }

    /** Use when the field's contract forbids null; fails fast if the wire disagrees. */
    public String readString() {
        String s = readNullableString();
        if (s == null) {
            throw new IllegalStateException("expected non-null STRING but wire encoded null");
        }
        return s;
    }

    /** COMPACT_STRING / COMPACT_NULLABLE_STRING: UNSIGNED_VARINT of (length+1); 0 = null. */
    public String readCompactNullableString() {
        int lengthPlusOne = readUnsignedVarint();
        if (lengthPlusOne == 0) {
            return null;
        }
        return new String(readRawBytes(lengthPlusOne - 1), StandardCharsets.UTF_8);
    }

    public String readCompactString() {
        String s = readCompactNullableString();
        if (s == null) {
            throw new IllegalStateException("expected non-null COMPACT_STRING but wire encoded null");
        }
        return s;
    }

    // ---- bytes ----

    /** BYTES / NULLABLE_BYTES: INT32 length, then raw bytes; -1 length = null. */
    public byte[] readNullableBytes() {
        int length = readInt32();
        if (length < 0) {
            return null;
        }
        return readRawBytes(length);
    }

    /** COMPACT_BYTES / COMPACT_NULLABLE_BYTES: UNSIGNED_VARINT of (length+1); 0 = null. */
    public byte[] readCompactNullableBytes() {
        int lengthPlusOne = readUnsignedVarint();
        if (lengthPlusOne == 0) {
            return null;
        }
        return readRawBytes(lengthPlusOne - 1);
    }

    private byte[] readRawBytes(int length) {
        byte[] out = new byte[length];
        buf.get(out);
        return out;
    }

    // ---- arrays ----

    /** ARRAY: INT32 count, then elements; -1 = null. */
    public <T> List<T> readArray(Function<ProtocolReader, T> elementReader) {
        int count = readInt32();
        if (count < 0) {
            return null;
        }
        List<T> out = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            out.add(elementReader.apply(this));
        }
        return out;
    }

    /** COMPACT_ARRAY: UNSIGNED_VARINT of (count+1), then elements; 0 = null. */
    public <T> List<T> readCompactArray(Function<ProtocolReader, T> elementReader) {
        int countPlusOne = readUnsignedVarint();
        if (countPlusOne == 0) {
            return null;
        }
        int count = countPlusOne - 1;
        List<T> out = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            out.add(elementReader.apply(this));
        }
        return out;
    }

    // ---- misc ----

    public UUID readUuid() {
        long msb = readInt64();
        long lsb = readInt64();
        return new UUID(msb, lsb);
    }

    /**
     * TAG_BUFFER: UNSIGNED_VARINT count, then that many {tag, size, data}
     * triples. None of the APIs we implement at the versions we implement
     * define any tags we act on, so every tag we encounter here is by
     * definition unknown to us — the wire format's own forward-compatibility
     * rule is to skip it by its declared size, not to treat it as an error.
     */
    public void readTagBuffer() {
        int tagCount = readUnsignedVarint();
        for (int i = 0; i < tagCount; i++) {
            readUnsignedVarint(); // tag id — unused, we don't recognize any
            int size = readUnsignedVarint();
            buf.position(buf.position() + size); // skip the tag's data verbatim
        }
    }
}
