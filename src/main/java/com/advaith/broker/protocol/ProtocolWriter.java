package com.advaith.broker.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.function.BiConsumer;

/**
 * Encodes Kafka primitive wire types into a growable buffer. Deliberately
 * hand-rolled (allocate bigger, flip the old buffer to read mode, copy it
 * in) instead of java.io.ByteArrayOutputStream — growing a ByteBuffer by
 * hand is the same flip/position discipline the network layer needed, and
 * that's the mechanic this project exists to practice (PRD §3: no buffer
 * wrapper libraries).
 */
public final class ProtocolWriter {

    private ByteBuffer buf;

    public ProtocolWriter() {
        this(64);
    }

    public ProtocolWriter(int initialCapacity) {
        this.buf = ByteBuffer.allocate(initialCapacity);
    }

    private void ensureCapacity(int additionalBytes) {
        if (buf.remaining() >= additionalBytes) {
            return;
        }
        int newCapacity = Math.max(buf.capacity() * 2, buf.position() + additionalBytes);
        ByteBuffer grown = ByteBuffer.allocate(newCapacity);
        buf.flip();       // switch the old buffer to read mode so its written bytes are visible...
        grown.put(buf);   // ...then copy them into the new, bigger one, which stays in write mode
        buf = grown;
    }

    // ---- fixed-width ----

    public void writeInt8(byte v) {
        ensureCapacity(1);
        buf.put(v);
    }

    public void writeInt16(short v) {
        ensureCapacity(2);
        buf.putShort(v);
    }

    public void writeInt32(int v) {
        ensureCapacity(4);
        buf.putInt(v);
    }

    public void writeInt64(long v) {
        ensureCapacity(8);
        buf.putLong(v);
    }

    public void writeBoolean(boolean v) {
        writeInt8((byte) (v ? 1 : 0));
    }

    // ---- varints ----

    /** Mirrors ProtocolReader#readUnsignedVarint — see that method for the encoding rationale. */
    public void writeUnsignedVarint(int value) {
        // Loop while more than 7 bits remain unwritten; emit sets of 7 bits
        // low-to-high, marking every non-final byte's continuation bit.
        while ((value & ~0x7F) != 0) {
            writeInt8((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        writeInt8((byte) value);
    }

    public void writeVarint(int value) {
        // Zigzag encode: interleave positive and negative numbers so small
        // negatives stay small on the wire (see ProtocolReader#readVarint).
        int zigzagged = (value << 1) ^ (value >> 31);
        writeUnsignedVarint(zigzagged);
    }

    public void writeVarlong(long value) {
        long zigzagged = (value << 1) ^ (value >> 63);
        while ((zigzagged & ~0x7FL) != 0) {
            writeInt8((byte) ((zigzagged & 0x7F) | 0x80));
            zigzagged >>>= 7;
        }
        writeInt8((byte) zigzagged);
    }

    // ---- strings ----

    public void writeNullableString(String s) {
        if (s == null) {
            writeInt16((short) -1);
            return;
        }
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        writeInt16((short) bytes.length);
        writeRawBytes(bytes);
    }

    public void writeString(String s) {
        if (s == null) {
            throw new IllegalArgumentException("STRING field must not be null (use writeNullableString)");
        }
        writeNullableString(s);
    }

    public void writeCompactNullableString(String s) {
        if (s == null) {
            writeUnsignedVarint(0);
            return;
        }
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        writeUnsignedVarint(bytes.length + 1);
        writeRawBytes(bytes);
    }

    public void writeCompactString(String s) {
        if (s == null) {
            throw new IllegalArgumentException("COMPACT_STRING field must not be null (use writeCompactNullableString)");
        }
        writeCompactNullableString(s);
    }

    // ---- bytes ----

    public void writeNullableBytes(byte[] bytes) {
        if (bytes == null) {
            writeInt32(-1);
            return;
        }
        writeInt32(bytes.length);
        writeRawBytes(bytes);
    }

    public void writeCompactNullableBytes(byte[] bytes) {
        if (bytes == null) {
            writeUnsignedVarint(0);
            return;
        }
        writeUnsignedVarint(bytes.length + 1);
        writeRawBytes(bytes);
    }

    public void writeRawBytes(byte[] bytes) {
        ensureCapacity(bytes.length);
        buf.put(bytes);
    }

    // ---- arrays ----

    public <T> void writeArray(List<T> items, BiConsumer<ProtocolWriter, T> elementWriter) {
        if (items == null) {
            writeInt32(-1);
            return;
        }
        writeInt32(items.size());
        for (T item : items) {
            elementWriter.accept(this, item);
        }
    }

    public <T> void writeCompactArray(List<T> items, BiConsumer<ProtocolWriter, T> elementWriter) {
        if (items == null) {
            writeUnsignedVarint(0);
            return;
        }
        writeUnsignedVarint(items.size() + 1);
        for (T item : items) {
            elementWriter.accept(this, item);
        }
    }

    // ---- misc ----

    public void writeUuid(UUID uuid) {
        writeInt64(uuid.getMostSignificantBits());
        writeInt64(uuid.getLeastSignificantBits());
    }

    /**
     * We never emit any tagged fields in M1 (none of our APIs define ones we
     * need), so every TAG_BUFFER we write is empty — but a flexible header
     * or body still requires the zero-count marker to be present.
     */
    public void writeEmptyTagBuffer() {
        writeUnsignedVarint(0);
    }

    /** Finalizes the buffer into an exact-length array of everything written so far. */
    public byte[] toByteArray() {
        // duplicate() shares the backing bytes but has its own independent
        // position/limit, so flipping it to read mode doesn't disturb this
        // writer's own position — more can still be written after this call.
        ByteBuffer view = buf.duplicate();
        view.flip();
        byte[] out = new byte[view.remaining()];
        view.get(out);
        return out;
    }
}
