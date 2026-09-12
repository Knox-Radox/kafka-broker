package com.advaith.broker.testsupport;

import com.advaith.broker.protocol.ProtocolReader;
import com.advaith.broker.protocol.ProtocolWriter;
import com.advaith.broker.record.Crc32C;

import java.util.ArrayList;
import java.util.List;

/**
 * Real, CRC-valid v2 RecordBatch construction/decoding for M5's chaos test
 * and benchmark — the multi-record sibling of {@link
 * com.advaith.broker.record.SimpleRecordCodec}, which only ever builds
 * single-record batches (that's all M3's offset storage needed). A
 * benchmark measuring realistic throughput needs a real producer's own
 * batching (many records per Produce call, not one), and the chaos test's
 * read-back verification needs each record's own ASSIGNED offset (not just
 * its value) to check PRD §9.2's exact claim: "every offset the producer
 * ever received an acknowledgment for... with the correct value."
 */
public final class TestRecordBatches {

    private TestRecordBatches() {}

    /** One value per record, no keys — exactly what the benchmark needs (throughput of values arriving, not partitioning). */
    public static byte[] buildBatch(List<byte[]> values) {
        ProtocolWriter records = new ProtocolWriter();
        for (int i = 0; i < values.size(); i++) {
            ProtocolWriter recordBody = new ProtocolWriter();
            recordBody.writeInt8((byte) 0); // attributes
            recordBody.writeVarlong(0);     // timestampDelta
            recordBody.writeVarint(i);      // offsetDelta
            recordBody.writeVarint(-1);     // null key
            writeVarintBytes(recordBody, values.get(i));
            recordBody.writeVarint(0); // headerCount
            byte[] recordBytes = recordBody.toByteArray();
            records.writeVarint(recordBytes.length);
            records.writeRawBytes(recordBytes);
        }
        byte[] recordsBytes = records.toByteArray();

        int recordCount = values.size();
        ProtocolWriter crcCovered = new ProtocolWriter();
        crcCovered.writeInt16((short) 0); // attributes: no compression
        crcCovered.writeInt32(Math.max(recordCount - 1, 0)); // lastOffsetDelta
        long now = System.currentTimeMillis();
        crcCovered.writeInt64(now);
        crcCovered.writeInt64(now);
        crcCovered.writeInt64(-1L);
        crcCovered.writeInt16((short) -1);
        crcCovered.writeInt32(-1);
        crcCovered.writeInt32(recordCount);
        crcCovered.writeRawBytes(recordsBytes);
        byte[] crcCoveredBytes = crcCovered.toByteArray();

        int crc = Crc32C.compute(crcCoveredBytes, 0, crcCoveredBytes.length);

        ProtocolWriter full = new ProtocolWriter();
        full.writeInt64(0L); // baseOffset — broker patches this on append, same as any real producer's batch
        full.writeInt32(4 + 1 + 4 + crcCoveredBytes.length);
        full.writeInt32(0); // partitionLeaderEpoch
        full.writeInt8((byte) 2); // magic
        full.writeInt32(crc);
        full.writeRawBytes(crcCoveredBytes);
        return full.toByteArray();
    }

    public record OffsetValue(long offset, byte[] value) {}

    /** Decodes one or more concatenated raw batches (a Fetch response's records blob) into (assigned offset, value) pairs, in on-disk order. */
    public static List<OffsetValue> decodeWithOffsets(byte[] rawBatches) {
        List<OffsetValue> out = new ArrayList<>();
        ProtocolReader reader = new ProtocolReader(rawBatches);
        while (reader.remaining() > 0) {
            long baseOffset = reader.readInt64();
            reader.readInt32(); // batchLength
            reader.readInt32(); // partitionLeaderEpoch
            reader.readInt8();  // magic
            reader.readInt32(); // crc
            reader.readInt16(); // attributes
            reader.readInt32(); // lastOffsetDelta
            reader.readInt64(); // firstTimestamp
            reader.readInt64(); // maxTimestamp
            reader.readInt64(); // producerId
            reader.readInt16(); // producerEpoch
            reader.readInt32(); // baseSequence
            int recordCount = reader.readInt32();
            for (int i = 0; i < recordCount; i++) {
                reader.readVarint();  // record length
                reader.readInt8();    // attributes
                reader.readVarlong(); // timestampDelta
                int offsetDelta = reader.readVarint();
                readVarintBytes(reader); // key
                byte[] value = readVarintBytes(reader);
                int headerCount = reader.readVarint();
                for (int h = 0; h < headerCount; h++) {
                    readVarintBytes(reader);
                    readVarintBytes(reader);
                }
                out.add(new OffsetValue(baseOffset + offsetDelta, value == null ? new byte[0] : value));
            }
        }
        return out;
    }

    private static void writeVarintBytes(ProtocolWriter w, byte[] bytes) {
        if (bytes == null) {
            w.writeVarint(-1);
            return;
        }
        w.writeVarint(bytes.length);
        w.writeRawBytes(bytes);
    }

    private static byte[] readVarintBytes(ProtocolReader reader) {
        int length = reader.readVarint();
        if (length < 0) {
            return null;
        }
        return reader.readRawBytes(length);
    }
}
