package com.advaith.broker.group;

import com.advaith.broker.log.LogManager;
import com.advaith.broker.log.StorageConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PRD §7.7 acceptance criterion 6: a commit must survive a restart. Here
 * "restart" means closing the LogManager (flushing every segment) and
 * opening a fresh one against the same directory — the same simulated-
 * restart pattern PartitionLogTest already uses for M2's crash recovery,
 * proving OffsetStore's in-memory materialized view is really rebuilt
 * from disk, not just holding onto references from before.
 */
class OffsetStoreTest {

    private static final int PARTITIONS = 4;

    @TempDir
    Path tempDir;

    private StorageConfig storageConfig() {
        return new StorageConfig(10 * 1024 * 1024, 4096, StorageConfig.UNLIMITED, StorageConfig.UNLIMITED, 1, StorageConfig.UNLIMITED);
    }

    private LogManager openLogManager() {
        return new LogManager(Map.of(GroupCoordinator.OFFSETS_TOPIC, PARTITIONS), tempDir, storageConfig());
    }

    @Test
    void aCommitSurvivesLogManagerBeingClosedAndReopened() {
        LogManager first = openLogManager();
        OffsetStore store = new OffsetStore(first, PARTITIONS);
        store.commit("my-group", "orders", 2, 42L, "some metadata");
        first.close();

        LogManager second = openLogManager();
        OffsetStore reloaded = new OffsetStore(second, PARTITIONS);

        Optional<OffsetStore.CommittedOffset> result = reloaded.fetch("my-group", "orders", 2);
        assertTrue(result.isPresent(), "a commit made before the restart must still be there after reloading from disk");
        assertEquals(42L, result.get().offset());
        assertEquals("some metadata", result.get().metadata());
    }

    @Test
    void laterCommitsForTheSamePartitionOverwriteEarlierOnesAfterReload() {
        LogManager first = openLogManager();
        OffsetStore store = new OffsetStore(first, PARTITIONS);
        store.commit("my-group", "orders", 0, 10L, "first");
        store.commit("my-group", "orders", 0, 20L, "second");
        first.close();

        LogManager second = openLogManager();
        OffsetStore reloaded = new OffsetStore(second, PARTITIONS);

        OffsetStore.CommittedOffset result = reloaded.fetch("my-group", "orders", 0).orElseThrow();
        assertEquals(20L, result.offset(), "replaying the log in order must leave the LATEST commit as the current value, not the first");
    }

    @Test
    void differentGroupsAndPartitionsDoNotCollideEvenWhenRoutedToTheSameInternalPartition() {
        LogManager logManager = openLogManager();
        OffsetStore store = new OffsetStore(logManager, PARTITIONS);
        store.commit("group-a", "topic-x", 0, 1L, null);
        store.commit("group-a", "topic-x", 1, 2L, null);
        store.commit("group-b", "topic-x", 0, 99L, null);

        assertEquals(1L, store.fetch("group-a", "topic-x", 0).orElseThrow().offset());
        assertEquals(2L, store.fetch("group-a", "topic-x", 1).orElseThrow().offset());
        assertEquals(99L, store.fetch("group-b", "topic-x", 0).orElseThrow().offset());
    }

    @Test
    void fetchingAnUncommittedPartitionReturnsEmptyNotAnError() {
        LogManager logManager = openLogManager();
        OffsetStore store = new OffsetStore(logManager, PARTITIONS);
        assertTrue(store.fetch("nobody-ever-committed", "orders", 0).isEmpty());
    }
}
