package com.advaith.broker.record;

import com.advaith.broker.protocol.Errors;
import com.advaith.broker.protocol.ProtocolReader;

import java.nio.ByteBuffer;

/**
 * Parses and validates a v2 RecordBatch header (PRD §5.4) far enough to
 * learn how many offsets it consumes — nothing more. Design decision: M1
 * stores and returns batches verbatim (PRD's explicit simplification), so
 * this class deliberately never decodes the individual records inside.
 */
public final class RecordBatch {

    public static final byte SUPPORTED_MAGIC = 2;

    /** Fixed header size: everything up to and including recordCount, before the records array starts. */
    private static final int HEADER_SIZE = 61;

    /** Raised when a batch fails validation; carries the Kafka error code the caller (ProduceHandler) should return. */
    public static final class InvalidRecordBatchException extends RuntimeException {
        public final short errorCode;

        public InvalidRecordBatchException(short errorCode, String message) {
            super(message);
            this.errorCode = errorCode;
        }
    }

    private final int recordCount;

    private RecordBatch(int recordCount) {
        this.recordCount = recordCount;
    }

    /** How many offsets this batch consumes — the broker's log advances its end offset by exactly this much. */
    public int recordCount() {
        return recordCount;
    }

    public static RecordBatch parse(byte[] batchBytes) {
        if (batchBytes.length < HEADER_SIZE) {
            throw new InvalidRecordBatchException(Errors.CORRUPT_MESSAGE,
                    "batch is shorter than the fixed RecordBatch header (" + batchBytes.length + " bytes)");
        }

        ProtocolReader reader = new ProtocolReader(batchBytes);
        reader.readInt64(); // baseOffset — producer always sends 0; broker patches it later, see rewriteBaseOffset()

        int batchLength = reader.readInt32();
        int expectedBatchLength = batchBytes.length - 12; // field's own contract: "bytes after this field"
        if (batchLength != expectedBatchLength) {
            throw new InvalidRecordBatchException(Errors.CORRUPT_MESSAGE,
                    "batchLength " + batchLength + " does not match actual size " + expectedBatchLength);
        }

        reader.readInt32(); // partitionLeaderEpoch — meaningless with one broker; read for M4
        byte magic = reader.readInt8();
        if (magic != SUPPORTED_MAGIC) {
            throw new InvalidRecordBatchException(Errors.CORRUPT_MESSAGE,
                    "unsupported magic byte " + magic + " (only v2 record batches are accepted)");
        }

        int storedCrc = reader.readInt32();
        // CRC coverage starts exactly here (the first byte of `attributes`)
        // and runs to the end of the batch. It deliberately excludes
        // baseOffset and partitionLeaderEpoch — fields the broker (or a
        // replica/proxy) is expected to rewrite after the producer computed
        // this checksum — plus batchLength/magic, which are outer framing,
        // not payload. Drawing the boundary here is exactly what makes
        // rewriteBaseOffset() below safe without recomputing the CRC.
        int crcCoverageStart = reader.position();
        int computedCrc = Crc32C.compute(batchBytes, crcCoverageStart, batchBytes.length - crcCoverageStart);
        if (computedCrc != storedCrc) {
            throw new InvalidRecordBatchException(Errors.CORRUPT_MESSAGE,
                    "CRC mismatch: stored=" + storedCrc + " computed=" + computedCrc);
        }

        short attributes = reader.readInt16();
        int compressionCodec = attributes & 0x07; // bits 0-2 (PRD §5.4)
        if (compressionCodec != 0) {
            throw new InvalidRecordBatchException(Errors.INVALID_RECORD,
                    "compression codec " + compressionCodec + " is not supported; produce uncompressed batches only");
        }

        reader.readInt32(); // lastOffsetDelta — redundant with recordCount for our purposes; unused in M1
        reader.readInt64(); // firstTimestamp — unused until retention-by-age (M2)
        reader.readInt64(); // maxTimestamp — unused in M1
        reader.readInt64(); // producerId — idempotence/transactions are a non-goal (PRD §2)
        reader.readInt16(); // producerEpoch — see above
        reader.readInt32(); // baseSequence — see above

        int recordCount = reader.readInt32();
        if (recordCount < 0) {
            throw new InvalidRecordBatchException(Errors.CORRUPT_MESSAGE, "negative recordCount " + recordCount);
        }

        return new RecordBatch(recordCount);
    }

    /**
     * Patches the baseOffset field of a stored batch in place with the
     * offset the broker actually assigned. Producers always send 0 —
     * only the broker's log knows the true next offset — and this is safe
     * post-hoc only because the CRC excludes baseOffset (see parse()).
     * ByteBuffer.wrap() aliases the given array rather than copying it, so
     * this mutates batchBytes directly; the caller's own reference sees the
     * change without needing anything returned.
     */
    public static void rewriteBaseOffset(byte[] batchBytes, long assignedBaseOffset) {
        ByteBuffer.wrap(batchBytes).putLong(0, assignedBaseOffset);
    }
}
