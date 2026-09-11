package com.advaith.broker.record;

import com.advaith.broker.protocol.Errors;
import com.advaith.broker.protocol.ProtocolWriter;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Builds a real v2 RecordBatch by hand (correct CRC-32C included) rather
 * than against a tcpdump-captured fixture — noted in JOURNAL.md as a
 * follow-up once a real producer is available to capture from (PRD §5.7).
 * Hand-building has one advantage this test relies on heavily: it lets us
 * deliberately corrupt one field at a time and know exactly what broke.
 */
class RecordBatchTest {

    @Test
    void parsesAValidTwoRecordBatch() {
        byte[] batch = buildBatch(2, (short) 0);
        RecordBatch parsed = RecordBatch.parse(batch);
        assertEquals(2, parsed.recordCount());
    }

    @Test
    void parsesAZeroRecordBatch() {
        byte[] batch = buildBatch(0, (short) 0);
        assertEquals(0, RecordBatch.parse(batch).recordCount());
    }

    @Test
    void rejectsWrongMagicByte() {
        byte[] batch = buildBatch(1, (short) 0);
        batch[16] = 1; // magic byte offset — see the HEADER_SIZE layout comment in RecordBatch
        var ex = assertThrows(RecordBatch.InvalidRecordBatchException.class, () -> RecordBatch.parse(batch));
        assertEquals(Errors.CORRUPT_MESSAGE, ex.errorCode);
    }

    @Test
    void rejectsCorruptedCrc() {
        byte[] batch = buildBatch(1, (short) 0);
        batch[batch.length - 1] ^= 0xFF; // flip a byte that's inside CRC coverage but not the CRC field itself
        var ex = assertThrows(RecordBatch.InvalidRecordBatchException.class, () -> RecordBatch.parse(batch));
        assertEquals(Errors.CORRUPT_MESSAGE, ex.errorCode);
    }

    @Test
    void rejectsNonZeroCompressionCodec() {
        byte[] batch = buildBatch(1, (short) 1); // codec bits = 1 (gzip)
        var ex = assertThrows(RecordBatch.InvalidRecordBatchException.class, () -> RecordBatch.parse(batch));
        assertEquals(Errors.INVALID_RECORD, ex.errorCode);
    }

    @Test
    void rejectsTruncatedBatch() {
        byte[] batch = new byte[10]; // shorter than the fixed 61-byte header
        var ex = assertThrows(RecordBatch.InvalidRecordBatchException.class, () -> RecordBatch.parse(batch));
        assertEquals(Errors.CORRUPT_MESSAGE, ex.errorCode);
    }

    @Test
    void rewriteBaseOffsetPatchesInPlaceWithoutBreakingTheCrc() {
        byte[] batch = buildBatch(1, (short) 0);
        RecordBatch.rewriteBaseOffset(batch, 12345L);

        // If the CRC boundary were wrong (e.g. it accidentally covered
        // baseOffset), this patch would have just corrupted the checksum.
        RecordBatch reparsed = RecordBatch.parse(batch);
        assertEquals(1, reparsed.recordCount());

        long baseOffset = java.nio.ByteBuffer.wrap(batch).getLong(0);
        assertEquals(12345L, baseOffset);
    }

    /** Hand-builds a real, CRC-valid v2 RecordBatch with `recordCount` minimal (null key/value) records. */
    private static byte[] buildBatch(int recordCount, short attributes) {
        ProtocolWriter records = new ProtocolWriter();
        for (int i = 0; i < recordCount; i++) {
            appendRecord(records, i);
        }
        byte[] recordsBytes = records.toByteArray();

        // Everything the CRC covers: attributes through the records array.
        ProtocolWriter crcCovered = new ProtocolWriter();
        crcCovered.writeInt16(attributes);
        crcCovered.writeInt32(Math.max(recordCount - 1, 0)); // lastOffsetDelta
        crcCovered.writeInt64(1_000L);                       // firstTimestamp
        crcCovered.writeInt64(1_000L + recordCount);         // maxTimestamp
        crcCovered.writeInt64(-1L);                          // producerId: -1 = no producer (non-transactional)
        crcCovered.writeInt16((short) -1);                   // producerEpoch: -1 = none
        crcCovered.writeInt32(-1);                            // baseSequence: -1 = none
        crcCovered.writeInt32(recordCount);
        crcCovered.writeRawBytes(recordsBytes);
        byte[] crcCoveredBytes = crcCovered.toByteArray();

        int crc = Crc32C.compute(crcCoveredBytes, 0, crcCoveredBytes.length);

        ProtocolWriter full = new ProtocolWriter();
        full.writeInt64(0L); // baseOffset — producer always sends 0
        int batchLength = 4 /* partitionLeaderEpoch */ + 1 /* magic */ + 4 /* crc */ + crcCoveredBytes.length;
        full.writeInt32(batchLength);
        full.writeInt32(0);              // partitionLeaderEpoch
        full.writeInt8((byte) 2);        // magic
        full.writeInt32(crc);
        full.writeRawBytes(crcCoveredBytes);
        return full.toByteArray();
    }

    private static void appendRecord(ProtocolWriter records, int offsetDelta) {
        ProtocolWriter body = new ProtocolWriter();
        body.writeInt8((byte) 0);       // record attributes — currently unused, always 0
        body.writeVarlong(offsetDelta); // timestampDelta — reuse offsetDelta for a simple, monotonic fixture value
        body.writeVarint(offsetDelta);
        body.writeVarint(-1);           // keyLength: -1 = null key
        body.writeVarint(-1);           // valueLength: -1 = null value
        body.writeVarint(0);            // headerCount: no headers
        byte[] bodyBytes = body.toByteArray();

        records.writeVarint(bodyBytes.length); // record's own length prefix
        records.writeRawBytes(bodyBytes);
    }
}
