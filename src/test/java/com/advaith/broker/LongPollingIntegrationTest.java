package com.advaith.broker;

import com.advaith.broker.api.ApiHandler;
import com.advaith.broker.api.ApiVersionsHandler;
import com.advaith.broker.api.FetchHandler;
import com.advaith.broker.api.ListOffsetsHandler;
import com.advaith.broker.api.MetadataHandler;
import com.advaith.broker.api.ProduceHandler;
import com.advaith.broker.api.RequestDispatcher;
import com.advaith.broker.log.LogManager;
import com.advaith.broker.log.StorageConfig;
import com.advaith.broker.network.NetworkServer;
import com.advaith.broker.protocol.ApiKey;
import com.advaith.broker.protocol.ProtocolReader;
import com.advaith.broker.protocol.ProtocolWriter;
import com.advaith.broker.record.Crc32C;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PRD §7.3 acceptance criterion 2: a Fetch with no data and a real
 * max_wait_ms must not return before that timeout elapses, and must
 * return promptly once a matching Produce supplies qualifying data.
 * Exercised over real sockets against the full handler stack — the same
 * standard FullRoundTripIntegrationTest holds itself to — because the
 * thing actually being proven here is timing behavior of the selector
 * loop itself (SelectorTicker's bounded select()), not just protocol
 * bytes.
 */
class LongPollingIntegrationTest {

    private static final int PORT = 28097;
    private static final String TOPIC = "test";

    @TempDir
    Path tempDir;

    private NetworkServer server;
    private Thread serverThread;

    private void startBroker() throws InterruptedException {
        BrokerConfig brokerConfig = new BrokerConfig(0, "127.0.0.1", PORT, "test-cluster");
        StorageConfig storageConfig = new StorageConfig(
                10 * 1024 * 1024, 4096, StorageConfig.UNLIMITED, StorageConfig.UNLIMITED, 1, StorageConfig.UNLIMITED);
        LogManager logManager = new LogManager(Map.of(TOPIC, 1), tempDir, storageConfig);
        FetchHandler fetchHandler = new FetchHandler(logManager, 10_000); // generous cap; each test sets its own max_wait_ms below it
        List<ApiHandler> handlers = List.of(
                new ApiVersionsHandler(),
                new MetadataHandler(brokerConfig, logManager),
                new ProduceHandler(logManager, fetchHandler),
                fetchHandler,
                new ListOffsetsHandler(logManager));

        server = new NetworkServer(PORT, new RequestDispatcher(handlers), fetchHandler);
        serverThread = new Thread(server, "long-polling-test-server");
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
    void fetchWithNoDataDoesNotReturnBeforeMaxWaitMsElapses() throws Exception {
        startBroker();
        try (Socket fetchSocket = connect()) {
            DataOutputStream out = new DataOutputStream(fetchSocket.getOutputStream());
            DataInputStream in = new DataInputStream(fetchSocket.getInputStream());

            long maxWaitMs = 1000;
            long start = System.currentTimeMillis();
            sendFrame(out, fetchRequest(1, TOPIC, 0, 0, (int) maxWaitMs, 1));
            byte[] responseBytes = readFrame(in);
            long elapsed = System.currentTimeMillis() - start;

            assertTrue(elapsed >= maxWaitMs - 50,
                    "a parked fetch must not resolve before its own max_wait_ms; took only " + elapsed + "ms");

            ProtocolReader response = new ProtocolReader(responseBytes);
            assertEquals(1, response.readInt32());
            response.readInt32(); // throttle_time_ms
            byte[] records = readSingleFetchRecords(response);
            assertEquals(0, records.length, "a timed-out long poll returns whatever's available — nothing here — never an error");
        }
    }

    @Test
    void fetchReturnsPromptlyOnceAMatchingProduceSuppliesData() throws Exception {
        startBroker();
        try (Socket fetchSocket = connect(); Socket produceSocket = connect()) {
            DataOutputStream fetchOut = new DataOutputStream(fetchSocket.getOutputStream());
            DataInputStream fetchIn = new DataInputStream(fetchSocket.getInputStream());
            DataOutputStream produceOut = new DataOutputStream(produceSocket.getOutputStream());
            DataInputStream produceIn = new DataInputStream(produceSocket.getInputStream());

            long maxWaitMs = 5000; // deliberately much longer than the produce delay below
            long start = System.currentTimeMillis();

            // Send the parked fetch on its own connection/thread — it blocks
            // on the socket read until the broker actually answers.
            CompletableFuture<byte[]> fetchResponse = CompletableFuture.supplyAsync(() -> {
                try {
                    sendFrame(fetchOut, fetchRequest(1, TOPIC, 0, 0, (int) maxWaitMs, 1));
                    return readFrame(fetchIn);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            Thread.sleep(300); // give the fetch time to actually park before the produce lands
            sendFrame(produceOut, produceRequest(2, TOPIC, 0, buildRecordBatch()));
            new ProtocolReader(readFrame(produceIn)); // drain the produce response; not the thing under test

            byte[] responseBytes = fetchResponse.get(java.util.concurrent.TimeUnit.SECONDS.toMillis(4), java.util.concurrent.TimeUnit.MILLISECONDS);
            long elapsed = System.currentTimeMillis() - start;

            assertTrue(elapsed < maxWaitMs - 1000,
                    "a produce landing on the watched partition must complete the parked fetch well before its timeout; took " + elapsed + "ms");

            ProtocolReader response = new ProtocolReader(responseBytes);
            assertEquals(1, response.readInt32());
            response.readInt32();
            byte[] records = readSingleFetchRecords(response);
            assertTrue(records.length > 0, "the parked fetch must be resolved WITH the record that was just produced, not an empty response");
        }
    }

    // ---- helpers ----

    private Socket connect() throws Exception {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress("localhost", PORT), 1000);
        return socket;
    }

    private static byte[] fetchRequest(int correlationId, String topic, int partition, long fetchOffset, int maxWaitMs, int minBytes) {
        ProtocolWriter w = header(ApiKey.FETCH, (short) 4, correlationId);
        w.writeInt32(-1); // replica_id
        w.writeInt32(maxWaitMs);
        w.writeInt32(minBytes);
        w.writeInt32(1_000_000); // max_bytes
        w.writeInt8((byte) 0); // isolation_level
        w.writeArray(List.of(topic), (tw, name) -> {
            tw.writeString(name);
            tw.writeArray(List.of(partition), (pw, idx) -> {
                pw.writeInt32(idx);
                pw.writeInt64(fetchOffset);
                pw.writeInt32(1_000_000);
            });
        });
        return w.toByteArray();
    }

    private static byte[] produceRequest(int correlationId, String topic, int partition, byte[] batch) {
        ProtocolWriter w = header(ApiKey.PRODUCE, (short) 3, correlationId);
        w.writeNullableString(null);
        w.writeInt16((short) 1); // acks=1
        w.writeInt32(30_000);
        w.writeArray(List.of(topic), (tw, name) -> {
            tw.writeString(name);
            tw.writeArray(List.of(partition), (pw, idx) -> {
                pw.writeInt32(idx);
                pw.writeNullableBytes(batch);
            });
        });
        return w.toByteArray();
    }

    private static ProtocolWriter header(ApiKey apiKey, short version, int correlationId) {
        ProtocolWriter w = new ProtocolWriter();
        w.writeInt16((short) apiKey.key);
        w.writeInt16(version);
        w.writeInt32(correlationId);
        w.writeNullableString("long-polling-test-client");
        return w;
    }

    private static byte[] readSingleFetchRecords(ProtocolReader r) {
        return r.readArray(tr -> {
            tr.readString();
            return tr.readArray(pr -> {
                pr.readInt32(); // partition_index
                assertEquals(0, pr.readInt16(), "error_code");
                pr.readInt64(); // high_watermark
                pr.readInt64(); // last_stable_offset
                pr.readArray(x -> null);
                byte[] bytes = pr.readNullableBytes();
                return bytes == null ? new byte[0] : bytes;
            }).get(0);
        }).get(0);
    }

    private static void sendFrame(DataOutputStream out, byte[] body) throws Exception {
        out.writeInt(body.length);
        out.write(body);
        out.flush();
    }

    private static byte[] readFrame(DataInputStream in) throws Exception {
        int length = in.readInt();
        byte[] body = new byte[length];
        in.readFully(body);
        return body;
    }

    private static byte[] buildRecordBatch() {
        ProtocolWriter recordBody = new ProtocolWriter();
        recordBody.writeInt8((byte) 0);
        recordBody.writeVarlong(0);
        recordBody.writeVarint(0);
        recordBody.writeVarint(-1); // null key
        recordBody.writeVarint(-1); // null value
        recordBody.writeVarint(0);  // no headers
        byte[] recordBytes = recordBody.toByteArray();

        ProtocolWriter records = new ProtocolWriter();
        records.writeVarint(recordBytes.length);
        records.writeRawBytes(recordBytes);
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
