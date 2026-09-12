package com.advaith.broker.chaos;

import com.advaith.broker.record.SimpleRecordCodec;
import com.advaith.broker.testsupport.RawProtocolClient;
import com.advaith.broker.testsupport.TestRecordBatches;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * PRD §9.2, run for real, exactly as specified: three real broker OS
 * processes, a continuous {@code acks=all} producer load, a real
 * {@code SIGKILL} of whichever broker is currently the partition's leader,
 * continued production against the survivor discovered via a fresh
 * Metadata call, and a full read-back that asserts — as a hard failure,
 * not a prose claim — that every offset the producer ever received an
 * acknowledgment for is present in the final log with the exact value it
 * was acked with. See STUDY_GUIDE.md §17 for the full writeup, including
 * gate 1's answer on exactly what this test does and doesn't cover.
 *
 * <p>Needs the real jar built first ({@code mvn -DskipTests package}) —
 * see the {@link Assumptions#assumeTrue} below for why a plain, from-clean
 * {@code mvn test} can't build it for us mid-run.
 */
class ChaosFailoverTest {

    private static final Logger log = LoggerFactory.getLogger(ChaosFailoverTest.class);
    private static final String TOPIC = "chaos";
    private static final int BROKER_COUNT = 3;
    private static final int TOTAL_MESSAGES = 200;
    private static final int KILL_AFTER_ACKS = 80;

    private final List<Process> allProcesses = new ArrayList<>();

    @AfterEach
    void killEverything() {
        for (Process p : allProcesses) {
            p.destroyForcibly();
        }
    }

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void killingTheLeaderMidLoadLosesNoAcknowledgedMessage(@TempDir Path tempDir) throws Exception {
        Path jar = Path.of("target/kafka-broker.jar");
        Assumptions.assumeTrue(Files.exists(jar),
                "target/kafka-broker.jar not built yet -- run `mvn -DskipTests package` first, then re-run "
                        + "(this test spawns the real jar as separate OS processes; `mvn test` alone runs before "
                        + "`package` in the Maven lifecycle, so a from-clean `mvn test` can't have built it yet)");

        // ---- 1. lay out a fresh 3-broker cluster on free ports, with fast-but-real lease timing ----
        int[] listenPorts = {findFreePort(), findFreePort(), findFreePort()};
        int[] metricsPorts = {findFreePort(), findFreePort(), findFreePort()};
        Map<Integer, Process> processByBrokerId = new LinkedHashMap<>();
        Map<Integer, RawProtocolClient.BrokerInfo> addressByBrokerId = new LinkedHashMap<>();

        for (int id = 0; id < BROKER_COUNT; id++) {
            addressByBrokerId.put(id, new RawProtocolClient.BrokerInfo(id, "127.0.0.1", listenPorts[id]));
        }

        for (int id = 0; id < BROKER_COUNT; id++) {
            Path configPath = writeBrokerConfig(tempDir, id, listenPorts, metricsPorts);
            Path logFile = tempDir.resolve("broker-" + id + ".log");
            ProcessBuilder pb = new ProcessBuilder("java", "-jar", jar.toAbsolutePath().toString(), configPath.toString());
            pb.redirectOutput(logFile.toFile());
            pb.redirectErrorStream(true);
            Process process = pb.start();
            allProcesses.add(process);
            processByBrokerId.put(id, process);
            log.info("started broker {} as pid {} on port {}, log at {}", id, process.pid(), listenPorts[id], logFile);
        }

        // ---- 2. wait for every broker to actually be answering requests ----
        for (int id = 0; id < BROKER_COUNT; id++) {
            waitUntilReachable(addressByBrokerId.get(id), 20_000);
        }
        log.info("all {} brokers are up and answering requests", BROKER_COUNT);

        // ---- 3. discover the initial leader, and wait for the cluster to actually finish forming ----
        RawProtocolClient.BrokerInfo currentLeader = discoverLeader(addressByBrokerId.values());
        log.info("initial leader of {}-0 is broker {}", TOPIC, currentLeader.nodeId());
        // Found for real via this exact test (JOURNAL.md, 2026-09-12): a
        // fast producer loop can rack up KILL_AFTER_ACKS's worth of
        // "acks=all" acknowledgments before EITHER follower's
        // PeerReplicator has completed even its first fetch round trip
        // against this brand-new leader -- meaning the ISR that "all" was
        // computed against was, for real, just [leader] the entire time,
        // making those early acks no stronger than acks=1 through no
        // fault of the replication code itself. A real operator would
        // never chaos-test a cluster mid-formation either; waiting here
        // for the ISR to actually reach every replica once is this test's
        // equivalent of "wait for the rebalance to settle" before trusting
        // any acknowledgment's strength.
        waitUntilFullyReplicated(currentLeader, 20_000);
        RawProtocolClient client = new RawProtocolClient(currentLeader.host(), currentLeader.port(), 5_000);

        // ---- 4. produce load, killing the leader partway through, retrying against whoever takes over ----
        Map<Long, byte[]> ackedOffsetToValue = new LinkedHashMap<>();
        boolean killed = false;
        long killedAtNanos = 0;
        int killedBrokerId = -1;

        for (int seq = 0; seq < TOTAL_MESSAGES; seq++) {
            byte[] value = ("m5-chaos-" + seq).getBytes(StandardCharsets.UTF_8);
            byte[] batch = SimpleRecordCodec.buildSingleRecordBatch(null, value);

            while (true) {
                try {
                    RawProtocolClient.ProduceResult result = client.produce(TOPIC, 0, batch, (short) -1, 15_000);
                    if (result.errorCode() == 0) {
                        ackedOffsetToValue.put(result.baseOffset(), value);
                        break;
                    }
                    log.info("produce for seq {} got error_code {}, refreshing leader and retrying", seq, result.errorCode());
                } catch (IOException e) {
                    log.info("produce for seq {} failed ({}), refreshing leader and retrying", seq, e.toString());
                }
                client.close();
                currentLeader = discoverLeader(addressByBrokerId.values());
                client = new RawProtocolClient(currentLeader.host(), currentLeader.port(), 5_000);
            }

            if (!killed && ackedOffsetToValue.size() >= KILL_AFTER_ACKS) {
                killedBrokerId = currentLeader.nodeId();
                Process victim = processByBrokerId.get(killedBrokerId);
                log.warn(">>> chaos: SIGKILL-ing broker {} (pid {}), the current leader, after {} acks <<<",
                        killedBrokerId, victim.pid(), ackedOffsetToValue.size());
                victim.destroyForcibly();
                killedAtNanos = System.nanoTime();
                killed = true;
            }
        }
        client.close();
        assertEquals(TOTAL_MESSAGES, ackedOffsetToValue.size(), "every message must eventually receive exactly one acknowledgment (possibly after a retry)");
        double failoverSeconds = killed ? (System.nanoTime() - killedAtNanos) / 1e9 : 0;
        log.info(">>> chaos: producer finished all {} messages; broker {} was killed with {} acks remaining to send, "
                        + "and production continued (retried, then succeeded) within roughly {} <<<",
                TOTAL_MESSAGES, killedBrokerId, TOTAL_MESSAGES - KILL_AFTER_ACKS, killed ? String.format("%.2fs", failoverSeconds) : "n/a");

        // ---- 5. read the ENTIRE partition back from offset 0 and verify every acked offset survived ----
        RawProtocolClient.BrokerInfo readerAddress = discoverLeader(addressByBrokerId.values());
        Map<Long, byte[]> actualOffsetToValue = new LinkedHashMap<>();
        try (RawProtocolClient reader = new RawProtocolClient(readerAddress.host(), readerAddress.port(), 5_000)) {
            long fetchOffset = 0;
            for (int guard = 0; guard < 1000; guard++) { // generous cap so a real bug hangs the test as a failure, not forever
                RawProtocolClient.FetchResult result = reader.fetch(TOPIC, 0, fetchOffset, 0, 10_000_000);
                assertEquals(0, result.errorCode(), "read-back fetch at offset " + fetchOffset + " must not error");
                if (result.records().length == 0) {
                    break; // caught up to the high-water mark: nothing left to read
                }
                for (TestRecordBatches.OffsetValue ov : TestRecordBatches.decodeWithOffsets(result.records())) {
                    actualOffsetToValue.put(ov.offset(), ov.value());
                    fetchOffset = Math.max(fetchOffset, ov.offset() + 1);
                }
            }
        }

        log.info(">>> chaos: read back {} total records from the final log; verifying all {} acknowledged offsets <<<",
                actualOffsetToValue.size(), ackedOffsetToValue.size());

        int verified = 0;
        for (Map.Entry<Long, byte[]> acked : ackedOffsetToValue.entrySet()) {
            byte[] actual = actualOffsetToValue.get(acked.getKey());
            if (actual == null) {
                fail("acknowledged offset " + acked.getKey() + " (value \"" + new String(acked.getValue(), StandardCharsets.UTF_8)
                        + "\") is MISSING from the final log -- this is exactly the acknowledged-message-loss claim this test exists to check");
            }
            assertArrayEquals(acked.getValue(), actual,
                    "acknowledged offset " + acked.getKey() + " has the WRONG value in the final log");
            verified++;
        }
        log.info(">>> chaos: PASS -- all {} acknowledged offsets verified present, in order, with the correct value <<<", verified);
    }

    // ==================== helpers ====================

    /**
     * Asks every reachable candidate who it believes leads (TOPIC, 0), then
     * VALIDATES that belief by actually trying to connect to the reported
     * leader before trusting it. This second step is not optional: a
     * survivor's own belief can lag behind reality by up to the lease
     * timeout (PRD §8.5) — right after killing the real leader, every
     * OTHER broker still correctly answers Metadata, but keeps reporting
     * the now-dead broker as leader until its own lease expires. Returning
     * that stale answer without checking it would hand the caller an
     * address nothing is listening on anymore.
     */
    private RawProtocolClient.BrokerInfo discoverLeader(Iterable<RawProtocolClient.BrokerInfo> candidates) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            for (RawProtocolClient.BrokerInfo candidate : candidates) {
                RawProtocolClient.BrokerInfo believedLeader;
                try (RawProtocolClient probe = new RawProtocolClient(candidate.host(), candidate.port(), 1_000)) {
                    believedLeader = probe.metadata(List.of(TOPIC)).leaderOf(TOPIC, 0);
                } catch (IOException | IllegalStateException e) {
                    continue; // this candidate is dead, or hasn't formed an opinion yet -- try the next
                }
                try (RawProtocolClient verify = new RawProtocolClient(believedLeader.host(), believedLeader.port(), 500)) {
                    verify.metadata(List.of(TOPIC));
                    return believedLeader; // reachable right now -- a genuinely current belief, not a stale one
                } catch (IOException stale) {
                    // candidate's belief hasn't caught up to reality yet -- try another candidate, or wait a round
                }
            }
            Thread.sleep(200);
        }
        throw new IllegalStateException("no broker reported a REACHABLE leader for " + TOPIC + "-0 within 30s -- something beyond an ordinary failover is wrong");
    }

    /** Blocks until (topic, 0)'s ISR, as reported by its own leader, includes all {@link #BROKER_COUNT} replicas -- see the call site's comment for why this matters. */
    private void waitUntilFullyReplicated(RawProtocolClient.BrokerInfo leader, long timeoutMs) throws InterruptedException, IOException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (System.nanoTime() < deadline) {
            try (RawProtocolClient probe = new RawProtocolClient(leader.host(), leader.port(), 2_000)) {
                int isrSize = probe.metadata(List.of(TOPIC)).isrSizeOf(TOPIC, 0);
                if (isrSize >= BROKER_COUNT) {
                    log.info("{}-0's ISR has reached all {} replicas -- cluster has genuinely finished forming", TOPIC, BROKER_COUNT);
                    return;
                }
            }
            Thread.sleep(100);
        }
        throw new IllegalStateException(TOPIC + "-0's ISR never reached " + BROKER_COUNT + " replicas within " + timeoutMs + "ms");
    }

    private void waitUntilReachable(RawProtocolClient.BrokerInfo address, long timeoutMs) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (System.nanoTime() < deadline) {
            try (RawProtocolClient probe = new RawProtocolClient(address.host(), address.port(), 500)) {
                probe.metadata(List.of(TOPIC));
                return;
            } catch (IOException e) {
                Thread.sleep(150);
            }
        }
        throw new IllegalStateException("broker " + address.nodeId() + " on port " + address.port() + " never became reachable within " + timeoutMs + "ms");
    }

    private static Path writeBrokerConfig(Path tempDir, int brokerId, int[] listenPorts, int[] metricsPorts) throws IOException {
        StringBuilder peers = new StringBuilder();
        for (int otherId = 0; otherId < BROKER_COUNT; otherId++) {
            if (otherId == brokerId) {
                continue;
            }
            if (peers.length() > 0) {
                peers.append(',');
            }
            peers.append(otherId).append(":127.0.0.1:").append(listenPorts[otherId]);
        }

        Path dataDir = tempDir.resolve("data-" + brokerId);
        Files.createDirectories(dataDir);

        String properties = String.join("\n",
                "broker.id=" + brokerId,
                "listen.port=" + listenPorts[brokerId],
                "advertised.host=127.0.0.1",
                "advertised.port=" + listenPorts[brokerId],
                "topics=" + TOPIC + ":1",
                "data.dir=" + dataDir,
                "log.segment.bytes=10485760",
                "log.index.interval.bytes=4096",
                "log.retention.bytes=-1",
                "log.retention.ms=-1",
                "log.flush.interval.messages=1",
                "log.flush.interval.ms=-1",
                "fetch.max.wait.ms=200",
                "offsets.topic.num.partitions=1",
                "group.min.session.timeout.ms=6000",
                "group.max.session.timeout.ms=300000",
                "group.rebalance.timeout.ms=60000",
                "group.initial.rebalance.delay.ms=0",
                "broker.peers=" + peers,
                "replication.factor=3",
                // Fast-but-real lease timing (JOURNAL.md, 2026-09-12: startup grace is
                // 3x this) -- keeps this test's real end-to-end runtime under a minute
                // instead of using the production defaults' multi-second windows.
                "replica.lag.time.max.ms=3000",
                "leader.lease.renew.interval.ms=300",
                "leader.lease.timeout.ms=1500",
                "replica.fetch.max.wait.ms=200",
                "replica.fetch.min.bytes=1",
                "metrics.port=" + metricsPorts[brokerId]
        );

        Path configPath = tempDir.resolve("broker-" + brokerId + ".properties");
        Files.writeString(configPath, properties);
        return configPath;
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
