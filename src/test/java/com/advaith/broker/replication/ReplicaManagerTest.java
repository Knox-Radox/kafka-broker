package com.advaith.broker.replication;

import com.advaith.broker.log.LogManager;
import com.advaith.broker.log.PartitionLog;
import com.advaith.broker.log.StorageConfig;
import com.advaith.broker.protocol.Errors;
import com.advaith.broker.protocol.ProtocolWriter;
import com.advaith.broker.record.Crc32C;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the startup-grace-period fix from JOURNAL.md (2026-09-12): a
 * broker that just started must not promote itself over a peer it simply
 * hasn't finished connecting to yet, even if that peer's per-partition
 * "last contact" already looks expired against a short lease timeout.
 */
class ReplicaManagerTest {

    @Test
    void doesNotPromoteWithinItsOwnStartupGracePeriod(@TempDir Path dataDir) throws InterruptedException {
        StorageConfig storageConfig = new StorageConfig(1024 * 1024, 4096, StorageConfig.UNLIMITED, StorageConfig.UNLIMITED, 1, StorageConfig.UNLIMITED);
        LogManager logManager = new LogManager(Map.of("orders", 1), dataDir, storageConfig);
        // A peer that will never actually answer (nothing listens on this
        // port) — enough to give this partition a real, non-trivial
        // multi-broker assignment without needing a second live broker.
        PeerInfo deadPeer = new PeerInfo(1, "127.0.0.1", 1); // port 1: reserved, always refused
        ReplicationConfig config = new ReplicationConfig(List.of(deadPeer), 2, 10_000, 50, 50, 500, 1);
        ReplicaManager replicaManager = new ReplicaManager(0, logManager, config);

        // A small real gap between construction (when the per-partition
        // lease clock starts) and start() (when the grace clock starts) —
        // long enough to exceed the 50ms lease timeout on its own, so a
        // missing grace period would show up as an immediate promotion.
        Thread.sleep(80);
        replicaManager.start();
        try {
            replicaManager.checkForPromotions();
            assertFalse(replicaManager.isLeaderFor("orders", 0),
                    "must not self-promote before its own startup grace period has even elapsed");

            Thread.sleep(200); // past both the (3x) grace period and the lease timeout
            replicaManager.checkForPromotions();
            assertTrue(replicaManager.isLeaderFor("orders", 0),
                    "should promote once the grace period AND the lease timeout have genuinely elapsed with no contact");
        } finally {
            replicaManager.stop();
        }
    }

    @Test
    void discardsAStaleRoundsRecordsInsteadOfMisaligningTheLog(@TempDir Path dataDir) {
        // Found via real multi-process testing, one layer deeper than the
        // epochStartOffset bug above (see JOURNAL.md, 2026-09-12): even
        // AFTER that fix, the very round whose response reveals a
        // truncation is itself carrying records fetched from the OLD,
        // now-stale offset. Appending them anyway silently re-numbers
        // "the leader's data from offset X" as "my own data starting at
        // wherever I just truncated to" — corrupting content while
        // looking perfectly caught up (same total offset range). The
        // fix: only ever append when the log's end still matches exactly
        // what was requested; otherwise drop this round and let the next
        // one re-request from the right place.
        StorageConfig storageConfig = new StorageConfig(1024 * 1024, 4096, StorageConfig.UNLIMITED, StorageConfig.UNLIMITED, 1, StorageConfig.UNLIMITED);
        LogManager logManager = new LogManager(Map.of("orders", 1), dataDir, storageConfig);
        PeerInfo peer = new PeerInfo(1, "127.0.0.1", 1);
        ReplicationConfig config = new ReplicationConfig(List.of(peer), 2, 10_000, 2_000, 6_000, 500, 1);
        ReplicaManager replicaManager = new ReplicaManager(0, logManager, config);

        PartitionLog log = logManager.getPartition("orders", 0).orElseThrow();
        log.append(buildBatch(), 1); // offset 0
        // Something else (a concurrent round, in the real system) already
        // advanced the log to offset 1 by the time this round's response
        // arrives — this round's own snapshot was taken back when the log
        // still ended at offset 0.
        long staleRequestedOffset = 0;
        assertEquals(1, log.logEndOffset());

        replicaManager.applyReplicaFetchResult(new TopicPartition("orders", 0), 1, Errors.NONE,
                0, 1, 0, staleRequestedOffset, buildBatch()); // same epoch (0) — no reconciliation involved, purely the staleness guard

        assertEquals(1, log.logEndOffset(),
                "a stale round's records must be dropped, not appended on top of a log end that already moved");
    }

    @Test
    void truncatesToEpochStartOffsetEvenWhenNotAheadOfTheReportedLeader(@TempDir Path dataDir) {
        // Found via real multi-process testing (see JOURNAL.md,
        // 2026-09-12): comparing raw logEndOffset counts is NOT enough to
        // detect divergence. Here our own log (offset 0..1, 2 records) is
        // SHORTER than the new leader's reported raw endpoint (5) — the
        // old, buggy "truncate only if I'm ahead of them" check would
        // never even look here — yet our own offset-1 record is still
        // wrong, because it was written locally under the OLD, now-
        // superseded epoch. Only epochStartOffset (the new leader's own
        // logEndOffset at the moment IT promoted itself) reveals that.
        StorageConfig storageConfig = new StorageConfig(1024 * 1024, 4096, StorageConfig.UNLIMITED, StorageConfig.UNLIMITED, 1, StorageConfig.UNLIMITED);
        LogManager logManager = new LogManager(Map.of("orders", 1), dataDir, storageConfig);
        PeerInfo peer = new PeerInfo(1, "127.0.0.1", 1); // never actually dialed in this test
        ReplicationConfig config = new ReplicationConfig(List.of(peer), 2, 10_000, 2_000, 6_000, 500, 1);
        ReplicaManager replicaManager = new ReplicaManager(0, logManager, config);

        PartitionLog log = logManager.getPartition("orders", 0).orElseThrow();
        log.append(buildBatch(), 1); // offset 0 — legitimate, shared history
        log.append(buildBatch(), 1); // offset 1 — OUR OWN diverged write, still stamped with the pre-promotion epoch
        assertEquals(2, log.logEndOffset());

        // Broker 1 responds as the real, current leader: epoch 1, which
        // began at offset 1 (it never had our offset-1 write) — its own
        // raw endpoint has since grown past ours (5 > 2), which is
        // exactly the shape that fools raw-endpoint comparison alone.
        replicaManager.applyReplicaFetchResult(new TopicPartition("orders", 0), 1, Errors.NONE,
                1, 1, 1, 2, null);

        assertEquals(1, log.logEndOffset(),
                "the diverged offset-1 write must be discarded once epoch 1 (starting at offset 1) is adopted, regardless of the leader's own raw endpoint");
    }

    /** Hand-builds a real, CRC-valid v2 RecordBatch with exactly one record — same construction PartitionLogTest/RecordBatchTest use. */
    private static byte[] buildBatch() {
        ProtocolWriter body = new ProtocolWriter();
        body.writeInt8((byte) 0);
        body.writeVarlong(0);
        body.writeVarint(0);
        body.writeVarint(-1);
        body.writeVarint(-1);
        body.writeVarint(0);
        byte[] bodyBytes = body.toByteArray();

        ProtocolWriter records = new ProtocolWriter();
        records.writeVarint(bodyBytes.length);
        records.writeRawBytes(bodyBytes);
        byte[] recordsBytes = records.toByteArray();

        ProtocolWriter crcCovered = new ProtocolWriter();
        crcCovered.writeInt16((short) 0);
        crcCovered.writeInt32(0);
        crcCovered.writeInt64(1_000L);
        crcCovered.writeInt64(1_000L);
        crcCovered.writeInt64(-1L);
        crcCovered.writeInt16((short) -1);
        crcCovered.writeInt32(-1);
        crcCovered.writeInt32(1);
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
