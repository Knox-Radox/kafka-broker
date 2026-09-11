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
import com.advaith.broker.record.RecordBatch;
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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Drives Metadata -> Produce -> Fetch -> ListOffsets over a real socket
 * against the full handler stack, with every request/response hand-built
 * and hand-checked — the same round trip acceptance criteria 2/3/5 (PRD
 * §5.6) describe, exercised before trusting a real Kafka client against it.
 */
class FullRoundTripIntegrationTest {

    private static final int PORT = 28095;
    private static final String TOPIC = "test";

    @TempDir
    Path tempDir;

    private NetworkServer server;
    private Thread serverThread;

    private void startBroker() throws InterruptedException {
        BrokerConfig brokerConfig = new BrokerConfig(0, "127.0.0.1", PORT, "test-cluster");
        // Segment size generously larger than this test's total data so
        // nothing rolls mid-test — segment rolling has its own dedicated
        // test (PartitionLogTest). flush=1 exercises the safest (and
        // default) durability policy on every single append.
        StorageConfig storageConfig = new StorageConfig(
                10 * 1024 * 1024, 4096, StorageConfig.UNLIMITED, StorageConfig.UNLIMITED, 1, StorageConfig.UNLIMITED);
        LogManager logManager = new LogManager(Map.of(TOPIC, 1), tempDir, storageConfig);
        List<ApiHandler> handlers = List.of(
                new ApiVersionsHandler(),
                new MetadataHandler(brokerConfig, logManager),
                new ProduceHandler(logManager),
                new FetchHandler(logManager),
                new ListOffsetsHandler(logManager));

        server = new NetworkServer(PORT, new RequestDispatcher(handlers));
        serverThread = new Thread(server, "round-trip-test-server");
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
    void metadataProduceFetchListOffsetsRoundTrip() throws Exception {
        startBroker();

        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("localhost", PORT), 1000);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());

            // --- Metadata: confirms the advertised host/port a real client would dial next ---
            sendFrame(out, metadataRequest(1, null));
            ProtocolReader metadataResponse = new ProtocolReader(readFrame(in));
            assertEquals(1, metadataResponse.readInt32(), "correlation_id");
            metadataResponse.readInt32(); // throttle_time_ms
            List<Object> brokers = metadataResponse.readArray(r -> {
                assertEquals(0, r.readInt32(), "node_id");
                assertEquals("127.0.0.1", r.readString(), "advertised host must be dialable, not a bind address");
                assertEquals(PORT, r.readInt32(), "advertised port");
                r.readNullableString(); // rack
                return null;
            });
            assertEquals(1, brokers.size());
            metadataResponse.readNullableString(); // cluster_id
            metadataResponse.readInt32(); // controller_id
            List<Object> topics = metadataResponse.readArray(r -> {
                assertEquals(0, r.readInt16(), "topic error_code");
                assertEquals(TOPIC, r.readString());
                r.readBoolean(); // is_internal
                List<Object> partitions = r.readArray(pr -> {
                    assertEquals(0, pr.readInt16());
                    assertEquals(0, pr.readInt32(), "partition_index");
                    assertEquals(0, pr.readInt32(), "leader_id");
                    pr.readArray(ProtocolReader::readInt32); // replica_nodes
                    pr.readArray(ProtocolReader::readInt32); // isr_nodes
                    return null;
                });
                assertEquals(1, partitions.size());
                return null;
            });
            assertEquals(1, topics.size());

            // --- Produce: one batch of 1 record, then one batch of 3 records ---
            byte[] batchA = buildRecordBatch(1);
            byte[] batchB = buildRecordBatch(3);

            sendFrame(out, produceRequest(2, TOPIC, 0, batchA, (short) 1));
            ProtocolReader produceA = new ProtocolReader(readFrame(in));
            assertEquals(2, produceA.readInt32());
            long baseOffsetA = readSingleProduceResult(produceA);
            assertEquals(0, baseOffsetA, "the log was empty, so the first batch must be assigned offset 0");
            // The bytes we sent had baseOffset=0 (producers always send 0); the broker
            // stores its OWN copy and patches THAT one, so our local `batchA` is
            // unaffected by the network round trip. Patch our copy the same way, using
            // the offset the broker actually reported, so the byte-for-byte comparisons
            // below check against what the broker should have produced — not a guess.
            RecordBatch.rewriteBaseOffset(batchA, baseOffsetA);

            sendFrame(out, produceRequest(3, TOPIC, 0, batchB, (short) 1));
            ProtocolReader produceB = new ProtocolReader(readFrame(in));
            assertEquals(3, produceB.readInt32());
            long baseOffsetB = readSingleProduceResult(produceB);
            assertEquals(1, baseOffsetB, "batch A consumed exactly offset 0; batch B must start at 1");
            RecordBatch.rewriteBaseOffset(batchB, baseOffsetB);

            // --- acks=0: must produce NO frame at all, and must not corrupt the correlation stream ---
            // acks=0 does not mean "don't write" — it means "don't respond". The record is
            // still appended (it consumes offset 4), which is exactly why every offset/
            // high_watermark assertion below is relative to a 5-record log, not 4.
            byte[] batchC = buildRecordBatch(1);
            long baseOffsetC = baseOffsetB + 3; // batch B held 3 records (offsets 1,2,3), so C must land at 4
            sendFrame(out, produceRequest(4, TOPIC, 0, batchC, (short) 0));
            sendFrame(out, metadataRequest(5, List.of(TOPIC))); // a second, distinguishable request right behind it
            ProtocolReader afterAcksZero = new ProtocolReader(readFrame(in));
            assertEquals(5, afterAcksZero.readInt32(),
                    "the very next frame off the wire must be request 5's response, proving acks=0 sent nothing");
            RecordBatch.rewriteBaseOffset(batchC, baseOffsetC); // patch our copy the same way, for the comparisons below

            // --- Fetch from offset 0: expect all three batches (A, B, C), concatenated verbatim ---
            sendFrame(out, fetchRequest(6, TOPIC, 0, 0, 1_000_000));
            ProtocolReader fetch0 = new ProtocolReader(readFrame(in));
            assertEquals(6, fetch0.readInt32());
            fetch0.readInt32(); // throttle_time_ms
            byte[] fetch0Records = readSingleFetchResult(fetch0, /*expectedHighWatermark*/ 5, /*expectedError*/ 0);
            byte[] expectedConcat = concat(concat(batchA, batchB), batchC);
            assertArrayEquals(expectedConcat, fetch0Records,
                    "batches are returned whole and in append order, with baseOffset already patched");

            // --- Fetch from offset 1: batch A (base 0, last 0) no longer qualifies; B and C remain ---
            sendFrame(out, fetchRequest(7, TOPIC, 0, 1, 1_000_000));
            ProtocolReader fetch1 = new ProtocolReader(readFrame(in));
            assertEquals(7, fetch1.readInt32());
            fetch1.readInt32();
            byte[] fetch1Records = readSingleFetchResult(fetch1, 5, 0);
            assertArrayEquals(concat(batchB, batchC), fetch1Records,
                    "fetching mid-log must return whole containing batches, not sliced");

            // --- Fetch at the log end: empty, not an error ---
            sendFrame(out, fetchRequest(8, TOPIC, 0, 5, 1_000_000));
            ProtocolReader fetchEnd = new ProtocolReader(readFrame(in));
            assertEquals(8, fetchEnd.readInt32());
            fetchEnd.readInt32();
            byte[] fetchEndRecords = readSingleFetchResult(fetchEnd, 5, 0);
            assertEquals(0, fetchEndRecords.length, "caught up to the tail must be empty, not an error");

            // --- Fetch before the log start: OFFSET_OUT_OF_RANGE ---
            sendFrame(out, fetchRequest(9, TOPIC, 0, -5, 1_000_000));
            ProtocolReader fetchBad = new ProtocolReader(readFrame(in));
            assertEquals(9, fetchBad.readInt32());
            fetchBad.readInt32();
            readSingleFetchResult(fetchBad, 5, 1); // error_code 1 = OFFSET_OUT_OF_RANGE

            // --- ListOffsets: earliest=0, latest=5 (three batches produced so far consumed offsets 0..4) ---
            sendFrame(out, listOffsetsRequest(10, TOPIC, 0, -2));
            ProtocolReader earliest = new ProtocolReader(readFrame(in));
            assertEquals(10, earliest.readInt32());
            assertEquals(0, readSingleListOffsetsResult(earliest));

            sendFrame(out, listOffsetsRequest(11, TOPIC, 0, -1));
            ProtocolReader latest = new ProtocolReader(readFrame(in));
            assertEquals(11, latest.readInt32());
            assertEquals(5, readSingleListOffsetsResult(latest));
        }
    }

    // ---- request builders: header v1 (client_id, no TAG_BUFFER) for all four of these APIs ----

    private static byte[] metadataRequest(int correlationId, List<String> topics) {
        ProtocolWriter w = header(ApiKey.METADATA, (short) 4, correlationId);
        w.writeArray(topics, (bw, name) -> bw.writeString(name));
        w.writeBoolean(false); // allow_auto_topic_creation — ignored either way
        return w.toByteArray();
    }

    private static byte[] produceRequest(int correlationId, String topic, int partition, byte[] batch, short acks) {
        ProtocolWriter w = header(ApiKey.PRODUCE, (short) 3, correlationId);
        w.writeNullableString(null); // transactional_id
        w.writeInt16(acks);
        w.writeInt32(30_000); // timeout_ms
        w.writeArray(List.of(topic), (tw, name) -> {
            tw.writeString(name);
            tw.writeArray(List.of(partition), (pw, idx) -> {
                pw.writeInt32(idx);
                pw.writeNullableBytes(batch);
            });
        });
        return w.toByteArray();
    }

    private static byte[] fetchRequest(int correlationId, String topic, int partition, long fetchOffset, int maxBytes) {
        ProtocolWriter w = header(ApiKey.FETCH, (short) 4, correlationId);
        w.writeInt32(-1); // replica_id
        w.writeInt32(0);  // max_wait_ms — ignored
        w.writeInt32(0);  // min_bytes — ignored
        w.writeInt32(maxBytes);
        w.writeInt8((byte) 0); // isolation_level: read_uncommitted
        w.writeArray(List.of(topic), (tw, name) -> {
            tw.writeString(name);
            tw.writeArray(List.of(partition), (pw, idx) -> {
                pw.writeInt32(idx);
                pw.writeInt64(fetchOffset);
                pw.writeInt32(maxBytes);
            });
        });
        return w.toByteArray();
    }

    private static byte[] listOffsetsRequest(int correlationId, String topic, int partition, long timestamp) {
        ProtocolWriter w = header(ApiKey.LIST_OFFSETS, (short) 1, correlationId);
        w.writeInt32(-1); // replica_id
        w.writeArray(List.of(topic), (tw, name) -> {
            tw.writeString(name);
            tw.writeArray(List.of(partition), (pw, idx) -> {
                pw.writeInt32(idx);
                pw.writeInt64(timestamp);
            });
        });
        return w.toByteArray();
    }

    private static ProtocolWriter header(ApiKey apiKey, short version, int correlationId) {
        ProtocolWriter w = new ProtocolWriter();
        w.writeInt16((short) apiKey.key);
        w.writeInt16(version);
        w.writeInt32(correlationId);
        w.writeNullableString("round-trip-test-client");
        return w;
    }

    // ---- response readers ----

    private static long readSingleProduceResult(ProtocolReader r) {
        List<Long> offsets = r.readArray(tr -> {
            tr.readString(); // topic name
            return tr.readArray(pr -> {
                pr.readInt32(); // index
                assertEquals(0, pr.readInt16(), "error_code");
                long baseOffset = pr.readInt64();
                pr.readInt64(); // log_append_time
                return baseOffset;
            }).get(0);
        });
        r.readInt32(); // throttle_time_ms
        return offsets.get(0);
    }

    private static byte[] readSingleFetchResult(ProtocolReader r, long expectedHighWatermark, int expectedError) {
        return r.readArray(tr -> {
            tr.readString(); // topic
            return tr.readArray(pr -> {
                pr.readInt32(); // partition_index
                assertEquals(expectedError, pr.readInt16(), "error_code");
                assertEquals(expectedHighWatermark, pr.readInt64(), "high_watermark");
                pr.readInt64(); // last_stable_offset
                pr.readArray(x -> null); // aborted_transactions
                byte[] records = pr.readNullableBytes();
                return records == null ? new byte[0] : records;
            }).get(0);
        }).get(0);
    }

    private static long readSingleListOffsetsResult(ProtocolReader r) {
        // No throttle_time_ms in a v1 ListOffsetsResponse — see the note in
        // ListOffsetsHandler and JOURNAL.md, 2026-09-11.
        return r.readArray(tr -> {
            tr.readString();
            return tr.readArray(pr -> {
                pr.readInt32(); // partition_index
                assertEquals(0, pr.readInt16(), "error_code");
                pr.readInt64(); // timestamp
                return pr.readInt64(); // offset
            }).get(0);
        }).get(0);
    }

    // ---- wire I/O + a real, CRC-valid RecordBatch builder (mirrors RecordBatchTest) ----

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

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] out = new byte[a.length + b.length];
        System.arraycopy(a, 0, out, 0, a.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }

    private static byte[] buildRecordBatch(int recordCount) {
        ProtocolWriter records = new ProtocolWriter();
        for (int i = 0; i < recordCount; i++) {
            ProtocolWriter body = new ProtocolWriter();
            body.writeInt8((byte) 0);
            body.writeVarlong(i);
            body.writeVarint(i);
            body.writeVarint(-1); // null key
            body.writeVarint(-1); // null value
            body.writeVarint(0);  // no headers
            byte[] bodyBytes = body.toByteArray();
            records.writeVarint(bodyBytes.length);
            records.writeRawBytes(bodyBytes);
        }
        byte[] recordsBytes = records.toByteArray();

        ProtocolWriter crcCovered = new ProtocolWriter();
        crcCovered.writeInt16((short) 0); // attributes: no compression
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
        full.writeInt64(0L); // baseOffset — producer always sends 0; the broker assigns and patches the real one
        full.writeInt32(4 + 1 + 4 + crcCoveredBytes.length);
        full.writeInt32(0); // partitionLeaderEpoch
        full.writeInt8((byte) 2); // magic
        full.writeInt32(crc);
        full.writeRawBytes(crcCoveredBytes);
        return full.toByteArray();
    }
}
