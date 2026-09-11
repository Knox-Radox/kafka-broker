package com.advaith.broker.record;

import java.util.zip.CRC32C;

/**
 * Thin wrapper around java.util.zip.CRC32C (JDK 9+) — the Castagnoli
 * polynomial Kafka's v2 record batch format actually uses. Deliberately not
 * java.util.zip.CRC32: same-looking API, different polynomial, and it would
 * compute a checksum every real Kafka client silently rejects.
 */
public final class Crc32C {
    private Crc32C() {}

    public static int compute(byte[] data, int offset, int length) {
        CRC32C crc = new CRC32C();
        crc.update(data, offset, length);
        // CRC32C#getValue() returns a long "unsigned 32-bit" value; the wire
        // field is a plain INT32, so truncating is just reinterpreting the
        // same 32 bits — not a loss of information.
        return (int) crc.getValue();
    }
}
