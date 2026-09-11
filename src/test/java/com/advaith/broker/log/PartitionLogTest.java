package com.advaith.broker.log;

import com.advaith.broker.protocol.ProtocolWriter;
import com.advaith.broker.record.Crc32C;
import com.advaith.broker.record.RecordBatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PRD §6's file-backed storage, tested the same way M1's in-memory version
 * was proven correct via FullRoundTripIntegrationTest: through the public
 * contract only (append/read/logEndOffset/logStartOffset), which is
 * exactly what stayed identical across the M1->M2 storage swap. The crash
 * recovery tests are the ones that matter most here — they're the direct
 * proof for PRD §6.4/acceptance criteria 3-4.
 */
class PartitionLogTest {

    @TempDir
    Path tempDir;

    @Test
    void appendAndReadRoundTripsLikeM1DidInMemory() {
        PartitionLog log = openLog(unlimitedSegments());
        byte[] batchA = buildBatch(1);
        byte[] batchB = buildBatch(2);

        long offsetA = log.append(batchA, 1);
        long offsetB = log.append(batchB, 2);

        assertEquals(0, offsetA);
        assertEquals(1, offsetB);
        assertEquals(3, log.logEndOffset());

        RecordBatch.rewriteBaseOffset(batchA, offsetA);
        RecordBatch.rewriteBaseOffset(batchB, offsetB);
        assertArrayEquals(concat(batchA, batchB), log.read(0, 1_000_000));
    }

    @Test
    void rollsIntoMultipleSegmentsOnceTheSizeThresholdIsCrossed() {
        // Deliberately tiny: one small batch alone should already exceed it.
        StorageConfig config = new StorageConfig(64, 16, StorageConfig.UNLIMITED, StorageConfig.UNLIMITED, 1, StorageConfig.UNLIMITED);
        PartitionLog log = openLog(config);

        for (int i = 0; i < 5; i++) {
            log.append(buildBatch(1), 1);
        }

        long logFileCount = countFilesWithExtension(".log");
        assertTrue(logFileCount > 1, "expected multiple .log files after crossing the segment size threshold, found " + logFileCount);

        // Reading across the whole log must still work seamlessly even
        // though the data now spans several segment files.
        byte[] everything = log.read(0, 1_000_000);
        assertTrue(everything.length > 0);
        assertEquals(5, log.logEndOffset());
    }

    @Test
    void recoversCleanlyAfterASimulatedRestartWithNoCorruption() throws IOException {
        StorageConfig config = unlimitedSegments();
        PartitionLog log = openLog(config);
        log.append(buildBatch(1), 1);
        log.append(buildBatch(3), 3);
        log.close();

        PartitionLog reopened = new PartitionLog(tempDir, config);
        assertEquals(4, reopened.logEndOffset(), "a clean restart must not lose or duplicate any offsets");
        assertEquals(0, reopened.logStartOffset());

        // And it must still accept new appends, continuing the offset
        // sequence exactly where the recovered state left off.
        long nextOffset = reopened.append(buildBatch(1), 1);
        assertEquals(4, nextOffset);
    }

    @Test
    void discardsATornWriteAtTheTailOnRecovery() throws IOException {
        StorageConfig config = unlimitedSegments();
        PartitionLog log = openLog(config);
        byte[] firstBatch = buildBatch(1);
        log.append(firstBatch, 1);                // offset 0 -- must survive
        byte[] lastBatch = buildBatch(1);
        log.append(lastBatch, 1);                 // offset 1 -- about to be torn
        log.close();

        // Simulate an unclean shutdown that caught write() mid-batch: chop
        // the file down partway through the SECOND batch, leaving the
        // first one fully intact. truncate() takes an absolute file
        // length, not a byte count to remove — the cut point is measured
        // from the start of the file, past the first batch entirely.
        Path activeSegmentLog = onlySegmentLogFile();
        truncateFile(activeSegmentLog, firstBatch.length + lastBatch.length / 2);
        long corruptedFileSize = Files.size(activeSegmentLog);

        PartitionLog recovered = new PartitionLog(tempDir, config);
        assertEquals(1, recovered.logEndOffset(),
                "the torn batch at offset 1 must be discarded; only the intact batch at offset 0 survives");

        // The file itself must actually be shortened on disk to end right
        // after the surviving batch, not just tracked as shorter in
        // memory — a future restart must see the same, already-clean file.
        assertTrue(Files.size(activeSegmentLog) < corruptedFileSize,
                "recovery must truncate away the dangling half-batch remnant, not just ignore it in memory");

        // New appends resume from the recovered offset, not the pre-crash one.
        long newOffset = recovered.append(buildBatch(1), 1);
        assertEquals(1, newOffset);
    }

    @Test
    void discardsABatchWithACorruptedChecksumOnRecovery() throws IOException {
        StorageConfig config = unlimitedSegments();
        PartitionLog log = openLog(config);
        log.append(buildBatch(1), 1); // offset 0 -- intact
        byte[] corrupted = buildBatch(1);
        log.append(corrupted, 1);     // offset 1 -- about to be corrupted, but left the RIGHT length
        log.close();

        // Flip a byte inside the CRC-covered region of the last batch,
        // without changing the file's length at all -- this is the "bytes
        // arrived but don't check out" case, distinct from a torn write.
        Path activeSegmentLog = onlySegmentLogFile();
        flipLastByte(activeSegmentLog);

        PartitionLog recovered = new PartitionLog(tempDir, config);
        assertEquals(1, recovered.logEndOffset(), "a batch that fails CRC validation must be discarded on recovery, same as a torn write");
    }

    @Test
    void sizeBasedRetentionDeletesOnlyClosedSegments() {
        // Tiny segments (one batch each) and a retention limit that only
        // ever allows roughly one segment's worth of closed data to survive.
        StorageConfig config = new StorageConfig(40, 16, 40, StorageConfig.UNLIMITED, 1, StorageConfig.UNLIMITED);
        PartitionLog log = openLog(config);

        for (int i = 0; i < 10; i++) {
            log.append(buildBatch(1), 1);
        }

        // The oldest data must be gone...
        assertThrows(PartitionLog.OffsetOutOfRangeException.class, () -> log.read(0, 1_000));
        // ...but the log end offset (nothing was ever un-appended) and the
        // most recent data must both still be intact and readable.
        assertEquals(10, log.logEndOffset());
        long start = log.logStartOffset();
        assertTrue(start > 0, "retention should have advanced the log start past offset 0");
        byte[] recent = log.read(start, 1_000);
        assertTrue(recent.length > 0);
    }

    private PartitionLog openLog(StorageConfig config) {
        return new PartitionLog(tempDir, config);
    }

    private static StorageConfig unlimitedSegments() {
        return new StorageConfig(10 * 1024 * 1024, 4096, StorageConfig.UNLIMITED, StorageConfig.UNLIMITED, 1, StorageConfig.UNLIMITED);
    }

    private long countFilesWithExtension(String extension) {
        try (Stream<Path> files = Files.list(tempDir)) {
            return files.filter(p -> p.toString().endsWith(extension)).count();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** Mirrors the on-disk naming convention from PRD §6.2: base offset, zero-padded to 20 digits. */
    private Path onlySegmentLogFile() throws IOException {
        try (Stream<Path> files = Files.list(tempDir)) {
            return files.filter(p -> p.toString().endsWith(".log")).findFirst()
                    .orElseThrow(() -> new IllegalStateException("no .log file found in " + tempDir));
        }
    }

    private static void truncateFile(Path path, int newLength) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            channel.truncate(newLength);
        }
    }

    private static void flipLastByte(Path path) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            long lastIndex = channel.size() - 1;
            ByteBuffer buf = ByteBuffer.allocate(1);
            channel.read(buf, lastIndex);
            buf.flip();
            byte flipped = (byte) (buf.get() ^ 0xFF);
            ByteBuffer out = ByteBuffer.wrap(new byte[] {flipped});
            channel.write(out, lastIndex);
        }
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] out = new byte[a.length + b.length];
        System.arraycopy(a, 0, out, 0, a.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }

    /** Hand-builds a real, CRC-valid v2 RecordBatch — same construction used in RecordBatchTest. */
    private static byte[] buildBatch(int recordCount) {
        ProtocolWriter records = new ProtocolWriter();
        for (int i = 0; i < recordCount; i++) {
            ProtocolWriter body = new ProtocolWriter();
            body.writeInt8((byte) 0);
            body.writeVarlong(i);
            body.writeVarint(i);
            body.writeVarint(-1);
            body.writeVarint(-1);
            body.writeVarint(0);
            byte[] bodyBytes = body.toByteArray();
            records.writeVarint(bodyBytes.length);
            records.writeRawBytes(bodyBytes);
        }
        byte[] recordsBytes = records.toByteArray();

        ProtocolWriter crcCovered = new ProtocolWriter();
        crcCovered.writeInt16((short) 0);
        crcCovered.writeInt32(Math.max(recordCount - 1, 0));
        crcCovered.writeInt64(1_000L);
        crcCovered.writeInt64(1_000L + recordCount);
        crcCovered.writeInt64(-1L);
        crcCovered.writeInt16((short) -1);
        crcCovered.writeInt32(-1);
        crcCovered.writeInt32(recordCount);
        crcCovered.writeRawBytes(recordsBytes);
        byte[] crcCoveredBytes = crcCovered.toByteArray();

        int crc = Crc32C.compute(crcCoveredBytes, 0, crcCoveredBytes.length);

        ProtocolWriter full = new ProtocolWriter();
        full.writeInt64(0L);
        full.writeInt32(4 + 1 + 4 + crcCoveredBytes.length);
        full.writeInt32(0);
        full.writeInt8((byte) 2);
        full.writeInt32(crc);
        full.writeRawBytes(crcCoveredBytes);
        return full.toByteArray();
    }
}
