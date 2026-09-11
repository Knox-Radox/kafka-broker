package com.advaith.broker.log;

import com.advaith.broker.protocol.ProtocolWriter;
import com.advaith.broker.record.Crc32C;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Hand-rolled benchmark for PRD §6.6's central question: what does
 * flush.messages=1 (fsync every record) actually cost, on this machine,
 * compared to a batched policy? No benchmarking framework — there's no
 * dependency budget for one (PRD §3) — just wall-clock timing around a
 * realistic run of appends, with p50/p99 computed by hand from a sorted
 * latency array. Deliberately a real @Test, not a disabled one-off script:
 * the whole point of §6.6 is "measure it yourself, on your own machine",
 * not "trust a number from someone else's machine or a textbook".
 */
class FsyncBenchmarkTest {

    private static final int APPENDS_PER_POLICY = 500;

    @TempDir
    Path tempDir;

    @Test
    void compareFsyncEveryRecordVersusBatchedFlushing() {
        BenchmarkResult perRecord = benchmark(tempDir.resolve("per-record"), 1);
        BenchmarkResult batched = benchmark(tempDir.resolve("batched"), 200);

        System.out.println("=== fsync benchmark (PRD §6.6), " + APPENDS_PER_POLICY + " appends per policy ===");
        System.out.println("flush.messages=1   (every record) : " + perRecord);
        System.out.println("flush.messages=200 (batched)      : " + batched);
        if (perRecord.throughputPerSecond() > 0) {
            System.out.printf("batched is %.1fx the throughput of per-record fsync on this machine%n",
                    batched.throughputPerSecond() / perRecord.throughputPerSecond());
        }

        // The one thing worth actually asserting: batched must never be
        // slower. The exact multiplier is for JOURNAL.md/STUDY_GUIDE.md to
        // record from THIS run — hard-coding an expected ratio here would
        // just be re-encoding someone else's disk into a test assertion.
        assertTrue(batched.throughputPerSecond() >= perRecord.throughputPerSecond(),
                "batched flushing should never be slower than fsync-per-record");
    }

    private BenchmarkResult benchmark(Path dir, int flushIntervalMessages) {
        StorageConfig config = new StorageConfig(
                64 * 1024 * 1024, 4096, StorageConfig.UNLIMITED, StorageConfig.UNLIMITED,
                flushIntervalMessages, StorageConfig.UNLIMITED);
        PartitionLog log = new PartitionLog(dir, config);

        long[] latenciesNanos = new long[APPENDS_PER_POLICY];
        long start = System.nanoTime();
        for (int i = 0; i < APPENDS_PER_POLICY; i++) {
            byte[] batch = buildBatch(1);
            long appendStart = System.nanoTime();
            log.append(batch, 1);
            latenciesNanos[i] = System.nanoTime() - appendStart;
        }
        long totalNanos = System.nanoTime() - start;
        log.close();

        Arrays.sort(latenciesNanos);
        double throughputPerSecond = APPENDS_PER_POLICY / (totalNanos / 1_000_000_000.0);
        long p50Micros = latenciesNanos[latenciesNanos.length / 2] / 1_000;
        long p99Micros = latenciesNanos[(int) (latenciesNanos.length * 0.99)] / 1_000;
        return new BenchmarkResult(throughputPerSecond, p50Micros, p99Micros);
    }

    private record BenchmarkResult(double throughputPerSecond, long p50Micros, long p99Micros) {
        @Override
        public String toString() {
            return String.format("%,.0f appends/sec, p50=%dµs, p99=%dµs", throughputPerSecond, p50Micros, p99Micros);
        }
    }

    /** Hand-builds a real, CRC-valid v2 RecordBatch — same construction used throughout the log tests. */
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
