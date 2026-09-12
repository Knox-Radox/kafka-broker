package com.advaith.broker.record;

import com.advaith.broker.protocol.ProtocolReader;
import com.advaith.broker.protocol.ProtocolWriter;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds and decodes single-record v2 RecordBatches carrying an arbitrary
 * key/value pair. Introduced for M3's consumer-group offset storage (PRD
 * §7.4): "consumer group offsets don't need a new storage mechanism, just
 * a specific, broker-managed topic using the identical one already proven
 * durable" — meaning OffsetStore's commits go through the exact same
 * PartitionLog.append()/read() path a client's own Produce/Fetch already
 * use, which means they have to actually BE valid RecordBatches, not just
 * arbitrary bytes. This class is the one place that knows how to wrap a
 * key/value pair into that shape and unwrap it again, so nothing above it
 * needs to touch CRCs or batch headers directly.
 */
public final class SimpleRecordCodec {

    private SimpleRecordCodec() {}

    public record Decoded(byte[] key, byte[] value) {}

    /** Wraps one key/value pair as a complete, CRC-valid, single-record RecordBatch — byte-for-byte the same shape RecordBatch.parse() accepts. */
    public static byte[] buildSingleRecordBatch(byte[] key, byte[] value) {
        ProtocolWriter recordBody = new ProtocolWriter();
        recordBody.writeInt8((byte) 0); // attributes — unused
        recordBody.writeVarlong(0);     // timestampDelta
        recordBody.writeVarint(0);      // offsetDelta — the only record in this batch
        writeVarintBytes(recordBody, key);
        writeVarintBytes(recordBody, value);
        recordBody.writeVarint(0); // headerCount
        byte[] recordBytes = recordBody.toByteArray();

        ProtocolWriter records = new ProtocolWriter();
        records.writeVarint(recordBytes.length);
        records.writeRawBytes(recordBytes);
        byte[] recordsBytes = records.toByteArray();

        ProtocolWriter crcCovered = new ProtocolWriter();
        crcCovered.writeInt16((short) 0); // attributes: no compression
        crcCovered.writeInt32(0);         // lastOffsetDelta
        long now = System.currentTimeMillis();
        crcCovered.writeInt64(now);       // firstTimestamp
        crcCovered.writeInt64(now);       // maxTimestamp
        crcCovered.writeInt64(-1L);       // producerId
        crcCovered.writeInt16((short) -1); // producerEpoch
        crcCovered.writeInt32(-1);         // baseSequence
        crcCovered.writeInt32(1);          // recordCount
        crcCovered.writeRawBytes(recordsBytes);
        byte[] crcCoveredBytes = crcCovered.toByteArray();

        int crc = Crc32C.compute(crcCoveredBytes, 0, crcCoveredBytes.length);

        ProtocolWriter full = new ProtocolWriter();
        full.writeInt64(0L); // baseOffset — PartitionLog.append() patches this to the real assigned offset, same as any other batch
        full.writeInt32(4 + 1 + 4 + crcCoveredBytes.length);
        full.writeInt32(0); // partitionLeaderEpoch
        full.writeInt8((byte) 2); // magic
        full.writeInt32(crc);
        full.writeRawBytes(crcCoveredBytes);
        return full.toByteArray();
    }

    /**
     * Decodes every record out of one or more concatenated raw batches
     * (exactly what {@code PartitionLog.read(...)} returns) — generic over
     * however many records each batch actually holds, even though
     * OffsetStore only ever writes single-record ones itself; a replay
     * that assumed "always 1" would silently misparse the moment that
     * assumption changed.
     */
    public static List<Decoded> decodeAll(byte[] rawBatches) {
        List<Decoded> out = new ArrayList<>();
        ProtocolReader reader = new ProtocolReader(rawBatches);
        while (reader.remaining() > 0) {
            reader.readInt64(); // baseOffset
            reader.readInt32(); // batchLength — recordCount below fully determines where this batch ends
            reader.readInt32(); // partitionLeaderEpoch
            reader.readInt8();  // magic
            reader.readInt32(); // crc — not re-verified on read (see RecordBatch.peekLocation's javadoc for why)
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
                reader.readVarint();  // offsetDelta
                byte[] key = readVarintBytes(reader);
                byte[] value = readVarintBytes(reader);
                int headerCount = reader.readVarint();
                for (int h = 0; h < headerCount; h++) {
                    readVarintBytes(reader);
                    readVarintBytes(reader);
                }
                out.add(new Decoded(key, value));
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
