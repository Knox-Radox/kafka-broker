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
import com.advaith.broker.metrics.Metrics;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PRD §7.2: proves multi-partition topics work end to end, and that
 * key-based partitioning is entirely the client's job. There is no
 * "route by key" code anywhere in this broker to test — a producer
 * decides {@code partition = hash(key) % numPartitions} for itself and
 * puts the answer straight in {@code partition_index} on the wire; the
 * broker's Produce/Fetch handlers just honor whatever index they're
 * given, exactly as they did for a single partition since M1. This test
 * plays the producer's role by hand (the same `hash(key) % n` a real
 * client's partitioner does) specifically to prove that role could live
 * entirely outside this codebase.
 */
class MultiPartitionIntegrationTest {

    private static final int PORT = 28096;
    private static final String TOPIC = "orders";
    private static final int PARTITION_COUNT = 4;

    @TempDir
    Path tempDir;

    private NetworkServer server;
    private Thread serverThread;

    private void startBroker() throws InterruptedException {
        BrokerConfig brokerConfig = new BrokerConfig(0, "127.0.0.1", PORT, "test-cluster");
        StorageConfig storageConfig = new StorageConfig(
                10 * 1024 * 1024, 4096, StorageConfig.UNLIMITED, StorageConfig.UNLIMITED, 1, StorageConfig.UNLIMITED);
        LogManager logManager = new LogManager(Map.of(TOPIC, PARTITION_COUNT), tempDir, storageConfig);
        FetchHandler fetchHandler = new FetchHandler(logManager);
        List<ApiHandler> handlers = List.of(
                new ApiVersionsHandler(),
                new MetadataHandler(brokerConfig, logManager),
                new ProduceHandler(logManager, fetchHandler),
                fetchHandler,
                new ListOffsetsHandler(logManager));

        server = new NetworkServer(PORT, new RequestDispatcher(handlers, new Metrics()), fetchHandler);
        serverThread = new Thread(server, "multi-partition-test-server");
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

    /**
     * The exact partitioning decision a real producer's default partitioner
     * makes (real Kafka uses murmur2 specifically; PRD §7.2 is explicit that
     * the exact hash is a client-library detail, not part of the wire
     * protocol — any deterministic hash proves the same point here).
     */
    private static int partitionFor(String key) {
        return Math.floorMod(key.hashCode(), PARTITION_COUNT);
    }

    @Test
    void metadataReportsAllFourPartitions() throws Exception {
        startBroker();
        try (Socket socket = connect()) {
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());

            sendFrame(out, metadataRequest(1, List.of(TOPIC)));
            ProtocolReader response = new ProtocolReader(readFrame(in));
            assertEquals(1, response.readInt32());
            response.readInt32(); // throttle_time_ms
            response.readArray(r -> { r.readInt32(); r.readString(); r.readInt32(); r.readNullableString(); return null; }); // brokers
            response.readNullableString(); // cluster_id
            response.readInt32(); // controller_id

            List<Object> topics = response.readArray(tr -> {
                assertEquals(0, tr.readInt16());
                assertEquals(TOPIC, tr.readString());
                tr.readBoolean(); // is_internal
                List<Object> partitions = tr.readArray(pr -> {
                    assertEquals(0, pr.readInt16());
                    int index = pr.readInt32();
                    assertEquals(0, pr.readInt32(), "leader_id — single broker is trivially the leader of every partition");
                    pr.readArray(ProtocolReader::readInt32);
                    pr.readArray(ProtocolReader::readInt32);
                    return index;
                });
                assertEquals(PARTITION_COUNT, partitions.size(), "orders must report all 4 configured partitions");
                assertEquals(List.of(0, 1, 2, 3), partitions);
                return null;
            });
            assertEquals(1, topics.size());
        }
    }

    @Test
    void keyedRecordsPartitionDeterministicallyAndOrderingHoldsWithinEachPartition() throws Exception {
        startBroker();
        try (Socket socket = connect()) {
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());

            // 20 keyed records across a handful of distinct keys. A real
            // producer would compute partitionFor(key) once per record and
            // send straight to that partition_index — that's all this loop
            // does. What's produced per partition, in order, is tracked so
            // we can assert exact within-partition ordering afterward.
            String[] keys = {"alice", "bob", "carol", "dave", "eve"};
            Map<Integer, List<String>> expectedByPartition = new HashMap<>();
            int correlationId = 1;
            for (int i = 0; i < 20; i++) {
                String key = keys[i % keys.length];
                int partition = partitionFor(key);
                String value = key + "-order-" + i;
                byte[] batch = buildRecordBatch(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));

                sendFrame(out, produceRequest(correlationId, TOPIC, partition, batch));
                ProtocolReader produceResponse = new ProtocolReader(readFrame(in));
                assertEquals(correlationId, produceResponse.readInt32());
                readProduceResult(produceResponse); // just drains + checks error_code == NONE
                correlationId++;

                expectedByPartition.computeIfAbsent(partition, p -> new ArrayList<>()).add(value);
            }

            // Same key always computed the same partition (proves the
            // *client's* partitioner, run by hand here, is deterministic —
            // not anything the broker enforces or even knows about).
            for (String key : keys) {
                assertEquals(partitionFor(key), partitionFor(key), "a deterministic hash must be stable across calls");
            }

            // Fetch each partition individually and confirm exact,
            // in-order values — cross-partition ordering is deliberately
            // never asserted here (PRD §7.2/§7.7: none is claimed).
            for (Map.Entry<Integer, List<String>> entry : expectedByPartition.entrySet()) {
                sendFrame(out, fetchRequest(correlationId, TOPIC, entry.getKey(), 0));
                ProtocolReader fetchResponse = new ProtocolReader(readFrame(in));
                assertEquals(correlationId, fetchResponse.readInt32());
                correlationId++;
                fetchResponse.readInt32(); // throttle_time_ms
                List<String> actualValues = readFetchValues(fetchResponse);
                assertEquals(entry.getValue(), actualValues,
                        "partition " + entry.getKey() + " must contain exactly its keys' records, in produce order");
            }
        }
    }

    // ---- request builders (header v1: client_id, no TAG_BUFFER) ----

    private static byte[] metadataRequest(int correlationId, List<String> topics) {
        ProtocolWriter w = header(ApiKey.METADATA, (short) 4, correlationId);
        w.writeArray(topics, ProtocolWriter::writeString);
        w.writeBoolean(false);
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

    private static byte[] fetchRequest(int correlationId, String topic, int partition, long fetchOffset) {
        ProtocolWriter w = header(ApiKey.FETCH, (short) 4, correlationId);
        w.writeInt32(-1);
        w.writeInt32(0);
        w.writeInt32(0);
        w.writeInt32(1_000_000);
        w.writeInt8((byte) 0);
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

    private static ProtocolWriter header(ApiKey apiKey, short version, int correlationId) {
        ProtocolWriter w = new ProtocolWriter();
        w.writeInt16((short) apiKey.key);
        w.writeInt16(version);
        w.writeInt32(correlationId);
        w.writeNullableString("multi-partition-test-client");
        return w;
    }

    private static void readProduceResult(ProtocolReader r) {
        r.readArray(tr -> {
            tr.readString();
            return tr.readArray(pr -> {
                pr.readInt32();
                assertEquals(0, pr.readInt16(), "error_code");
                pr.readInt64(); // base_offset
                pr.readInt64(); // log_append_time
                return null;
            });
        });
        r.readInt32(); // throttle_time_ms
    }

    private static List<String> readFetchValues(ProtocolReader r) {
        byte[] records = r.readArray(tr -> {
            tr.readString();
            return tr.readArray(pr -> {
                pr.readInt32();
                assertEquals(0, pr.readInt16(), "error_code");
                pr.readInt64(); // high_watermark
                pr.readInt64(); // last_stable_offset
                pr.readArray(x -> null);
                byte[] bytes = pr.readNullableBytes();
                return bytes == null ? new byte[0] : bytes;
            }).get(0);
        }).get(0);
        return decodeValues(records);
    }

    // ---- a real, CRC-valid single-record RecordBatch carrying an actual key/value ----

    private static byte[] buildRecordBatch(byte[] key, byte[] value) {
        ProtocolWriter recordBody = new ProtocolWriter();
        recordBody.writeInt8((byte) 0); // attributes
        recordBody.writeVarlong(0); // timestampDelta
        recordBody.writeVarint(0);  // offsetDelta
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
        crcCovered.writeInt32(0); // lastOffsetDelta
        crcCovered.writeInt64(1_000L);
        crcCovered.writeInt64(1_000L);
        crcCovered.writeInt64(-1L);
        crcCovered.writeInt16((short) -1);
        crcCovered.writeInt32(-1);
        crcCovered.writeInt32(1); // recordCount
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

    private static void writeVarintBytes(ProtocolWriter w, byte[] bytes) {
        if (bytes == null) {
            w.writeVarint(-1);
            return;
        }
        w.writeVarint(bytes.length);
        w.writeRawBytes(bytes);
    }

    /** Parses the concatenated raw batch bytes back into just their values, for assertion. */
    private static List<String> decodeValues(byte[] rawBatches) {
        List<String> values = new ArrayList<>();
        ProtocolReader reader = new ProtocolReader(rawBatches);
        while (reader.remaining() > 0) {
            reader.readInt64(); // baseOffset
            reader.readInt32(); // batchLength — recordCount fully determines where this batch ends, so unused here
            reader.readInt32(); // partitionLeaderEpoch
            reader.readInt8();  // magic
            reader.readInt32(); // crc
            reader.readInt16(); // attributes
            reader.readInt32(); // lastOffsetDelta
            reader.readInt64(); // firstTimestamp
            reader.readInt64(); // maxTimestamp
            reader.readInt64(); // producerId
            reader.readInt16(); // producerEpoch
            reader.readInt32(); // baseSequence
            int recordCount = reader.readInt32();
            for (int i = 0; i < recordCount; i++) {
                reader.readVarint(); // record length
                reader.readInt8();   // attributes
                reader.readVarlong(); // timestampDelta
                reader.readVarint();  // offsetDelta
                readVarintBytes(reader); // key — unused here, but must be consumed
                byte[] value = readVarintBytes(reader);
                int headerCount = reader.readVarint();
                for (int h = 0; h < headerCount; h++) {
                    readVarintBytes(reader);
                    readVarintBytes(reader);
                }
                values.add(new String(value, StandardCharsets.UTF_8));
            }
        }
        return values;
    }

    private static byte[] readVarintBytes(ProtocolReader reader) {
        int length = reader.readVarint();
        if (length < 0) {
            return new byte[0];
        }
        return reader.readRawBytes(length);
    }

    private Socket connect() throws Exception {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress("localhost", PORT), 1000);
        return socket;
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
}
