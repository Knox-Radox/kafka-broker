package com.advaith.broker.benchmark;

import com.advaith.broker.BrokerConfig;
import com.advaith.broker.api.ApiHandler;
import com.advaith.broker.api.ApiVersionsHandler;
import com.advaith.broker.api.FetchHandler;
import com.advaith.broker.api.ListOffsetsHandler;
import com.advaith.broker.api.MetadataHandler;
import com.advaith.broker.api.ProduceHandler;
import com.advaith.broker.api.RequestDispatcher;
import com.advaith.broker.log.LogManager;
import com.advaith.broker.log.StorageConfig;
import com.advaith.broker.metrics.Metrics;
import com.advaith.broker.network.NetworkServer;
import com.advaith.broker.record.SimpleRecordCodec;
import com.advaith.broker.testsupport.RawProtocolClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PRD §9.3: the WHOLE SYSTEM, through the real network protocol, the way a
 * real client actually experiences it — distinct from M2's
 * {@code FsyncBenchmarkTest}, which measured only the storage layer in
 * isolation. Drives increasing concurrent load against one real, embedded
 * broker (real {@code NetworkServer}, real sockets to localhost — the
 * broker doesn't need to be a separate OS process for a benchmark; that
 * requirement is specific to the chaos test's independent-failure premise,
 * see {@code ChaosFailoverTest}) until throughput stops increasing, then
 * reports what the server's own single selector thread was actually doing
 * at that point via real stack sampling — not a guess.
 *
 * <p>Run it yourself: {@code mvn test -Dtest=ThroughputBenchmarkTest} —
 * see BENCHMARKS.md for the methodology writeup and a real run's numbers.
 */
class ThroughputBenchmarkTest {

    private static final Logger log = LoggerFactory.getLogger(ThroughputBenchmarkTest.class);
    private static final String TOPIC = "bench";
    private static final int VALUE_SIZE_BYTES = 100;
    private static final long LEVEL_DURATION_MS = 1500;
    private static final int[] CONCURRENCY_LEVELS = {1, 2, 4, 8, 16, 32, 64, 128, 256};
    /** Below this fractional gain over the previous level, throughput is considered to have stopped increasing — PRD §9.3's "found empirically, not assumed." */
    private static final double SATURATION_GAIN_THRESHOLD = 0.10;

    @TempDir
    Path tempDir;

    private NetworkServer server;
    private Thread serverThread;
    private int port;

    private void startBroker() throws IOException, InterruptedException {
        port = findFreePort();
        BrokerConfig brokerConfig = new BrokerConfig(0, "127.0.0.1", port, "bench-cluster");
        // Batched fsync, NOT M2's own per-record default: this benchmark
        // measures the network/protocol-layer ceiling (PRD §9.3), and a
        // per-record fsync would just re-measure M2's own already-
        // documented ~120x storage-layer cost (BENCHMARKS.md's M2 entry),
        // swamping the thing this milestone is actually trying to find.
        StorageConfig storageConfig = new StorageConfig(
                64L * 1024 * 1024, 4096, StorageConfig.UNLIMITED, StorageConfig.UNLIMITED, 1000, StorageConfig.UNLIMITED);
        LogManager logManager = new LogManager(Map.of(TOPIC, 1), tempDir, storageConfig);
        FetchHandler fetchHandler = new FetchHandler(logManager);
        List<ApiHandler> handlers = List.of(
                new ApiVersionsHandler(),
                new MetadataHandler(brokerConfig, logManager),
                new ProduceHandler(logManager, fetchHandler),
                fetchHandler,
                new ListOffsetsHandler(logManager));

        server = new NetworkServer(port, new RequestDispatcher(handlers, new Metrics()), fetchHandler);
        serverThread = new Thread(server, "benchmark-server-selector");
        serverThread.setDaemon(true);
        serverThread.start();
        Thread.sleep(200);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (server != null) {
            server.stop();
            serverThread.join(2000);
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void measuresThroughputAndLatencyToSaturation() throws Exception {
        startBroker();
        byte[] value = new byte[VALUE_SIZE_BYTES];
        new Random(42).nextBytes(value);
        byte[] batch = SimpleRecordCodec.buildSingleRecordBatch(null, value);

        System.out.println("=== M5 whole-system throughput/latency benchmark (PRD §9.3) ===");
        System.out.printf("value size=%dB, acks=1, batched fsync (flush.interval.messages=1000), %dms measured per concurrency level%n",
                VALUE_SIZE_BYTES, LEVEL_DURATION_MS);

        double previousThroughput = 0;
        Result peak = null;
        Map<String, Long> peakStackSamples = null;

        for (int concurrency : CONCURRENCY_LEVELS) {
            Map<String, Long> stackSamples = new ConcurrentHashMap<>();
            AtomicBoolean sampling = new AtomicBoolean(true);
            Thread sampler = startStackSampler(stackSamples, sampling);

            Result result = runLevel(concurrency, batch);

            sampling.set(false);
            sampler.join();

            System.out.printf("concurrency=%3d : %9.0f records/sec, p50=%6.0fus p95=%6.0fus p99=%6.0fus%n",
                    concurrency, result.throughput, result.p50Micros, result.p95Micros, result.p99Micros);

            if (peak == null || result.throughput > peak.throughput) {
                peak = result;
                peakStackSamples = stackSamples;
            }

            if (previousThroughput > 0 && (result.throughput - previousThroughput) / previousThroughput < SATURATION_GAIN_THRESHOLD) {
                System.out.printf("saturation reached at concurrency=%d: throughput gain over the previous level fell below %.0f%%%n",
                        concurrency, SATURATION_GAIN_THRESHOLD * 100);
                break;
            }
            previousThroughput = result.throughput;
        }

        System.out.printf("=== peak throughput: %.0f records/sec at concurrency=%d ===%n", peak.throughput, peak.concurrency);
        System.out.println("=== real stack samples of the server's own selector thread during that level, top frames by sample count ===");
        peakStackSamples.entrySet().stream()
                .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
                .limit(5)
                .forEach(e -> System.out.printf("%6d samples : %s%n", e.getValue(), e.getKey()));

        assertTrue(peak.throughput > 0, "must have produced at least something");
    }

    private record Result(int concurrency, double throughput, double p50Micros, double p95Micros, double p99Micros) {}

    /** Runs {@code concurrency} producer threads, each on its own persistent connection, hammering Produce for a fixed wall-clock window. */
    private Result runLevel(int concurrency, byte[] batch) throws InterruptedException {
        ConcurrentLinkedQueue<Long> latenciesNanos = new ConcurrentLinkedQueue<>();
        AtomicLong totalRecords = new AtomicLong();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch startGate = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();

        for (int t = 0; t < concurrency; t++) {
            Thread thread = new Thread(() -> {
                try (RawProtocolClient client = new RawProtocolClient("127.0.0.1", port, 5_000)) {
                    startGate.await();
                    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(LEVEL_DURATION_MS);
                    long count = 0;
                    while (System.nanoTime() < deadline) {
                        long start = System.nanoTime();
                        client.produce(TOPIC, 0, batch, (short) 1, 5_000);
                        latenciesNanos.add(System.nanoTime() - start);
                        count++;
                    }
                    totalRecords.addAndGet(count);
                } catch (Exception e) {
                    failure.compareAndSet(null, e);
                }
            }, "bench-producer-" + t);
            threads.add(thread);
            thread.start();
        }

        long levelStartNanos = System.nanoTime();
        startGate.countDown();
        for (Thread thread : threads) {
            thread.join();
        }
        double elapsedSeconds = (System.nanoTime() - levelStartNanos) / 1e9;

        if (failure.get() != null) {
            throw new RuntimeException("a producer thread failed at concurrency=" + concurrency, failure.get());
        }

        long[] sorted = latenciesNanos.stream().mapToLong(Long::longValue).sorted().toArray();
        return new Result(concurrency, totalRecords.get() / elapsedSeconds,
                percentileMicros(sorted, 0.50), percentileMicros(sorted, 0.95), percentileMicros(sorted, 0.99));
    }

    private static double percentileMicros(long[] sortedNanos, double p) {
        if (sortedNanos.length == 0) {
            return 0;
        }
        int index = Math.max(0, Math.min(sortedNanos.length - 1, (int) Math.ceil(p * sortedNanos.length) - 1));
        return sortedNanos[index] / 1000.0;
    }

    /**
     * A hand-rolled sampling profiler (PRD §9.3: "profiled, not guessed" —
     * this project has no profiler dependency budget, same zero-dependency
     * stance as everywhere else, so this is the honest, simple alternative):
     * every 2ms, snapshot the server's own selector thread's real call
     * stack and tally the top frame. Whatever frame accumulates the most
     * samples is genuinely where that thread was spending most of its wall
     * time during the sampled window — the same principle every real
     * sampling profiler (async-profiler, JFR) uses, just without the
     * tooling.
     */
    private Thread startStackSampler(Map<String, Long> samples, AtomicBoolean keepSampling) {
        Thread sampler = new Thread(() -> {
            while (keepSampling.get()) {
                StackTraceElement[] trace = serverThread.getStackTrace();
                if (trace.length > 0) {
                    String key = trace[0].getClassName() + "." + trace[0].getMethodName();
                    samples.merge(key, 1L, Long::sum);
                }
                try {
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }, "stack-sampler");
        sampler.start();
        return sampler;
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
