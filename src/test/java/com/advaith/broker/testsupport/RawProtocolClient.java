package com.advaith.broker.testsupport;

import com.advaith.broker.protocol.ApiKey;
import com.advaith.broker.protocol.ProtocolReader;
import com.advaith.broker.protocol.ProtocolWriter;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;

/**
 * A minimal, hand-rolled Kafka wire client — the same request/response
 * building every {@code *IntegrationTest} in this project already does by
 * hand, pulled out once so M5's chaos test (§9.2) and benchmark (§9.3) can
 * both drive real requests over a real socket without re-deriving these
 * byte layouts a third and fourth time. Deliberately NOT a general-purpose
 * client: only Metadata/Produce/Fetch, only the one version of each this
 * broker itself speaks (PRD §5.1 — min == max per API), and no retry logic
 * of its own — retry/failover policy belongs to the caller (a chaos test's
 * retry-after-Metadata loop looks nothing like a benchmark's tight loop).
 */
public final class RawProtocolClient implements Closeable {

    private final Socket socket;
    private final DataOutputStream out;
    private final DataInputStream in;
    private int correlationId = 1;

    public RawProtocolClient(String host, int port, int connectTimeoutMs) throws IOException {
        socket = new Socket();
        socket.connect(new InetSocketAddress(host, port), connectTimeoutMs);
        socket.setSoTimeout(30_000);
        // Without this, Nagle's algorithm holds a small outgoing request
        // (waiting to see if more data will follow to coalesce into one
        // segment) while the OTHER side's delayed-ACK timer is doing the
        // same thing waiting for a reply to piggyback on — the classic
        // Nagle/delayed-ACK interaction, and it costs a flat ~40ms on
        // EVERY request-response round trip regardless of load (found for
        // real running ThroughputBenchmarkTest before this fix: identical
        // ~44ms latency at every concurrency level from 1 to 256, which is
        // itself the tell — a real server-side cost would scale with load,
        // a fixed client-socket artifact does not). This request/response
        // client sends one small frame and blocks for one small reply, the
        // exact shape Nagle actively hurts, so TCP_NODELAY isn't optional
        // here the way it might be for a streaming bulk transfer.
        socket.setTcpNoDelay(true);
        out = new DataOutputStream(socket.getOutputStream());
        in = new DataInputStream(socket.getInputStream());
    }

    public record BrokerInfo(int nodeId, String host, int port) {}

    public record PartitionMetadata(int partitionIndex, int leaderId, List<Integer> isrNodes) {}

    public record TopicMetadata(String name, List<PartitionMetadata> partitions) {}

    public record MetadataResult(List<BrokerInfo> brokers, List<TopicMetadata> topics) {
        /** Convenience: the advertised host/port of whichever broker this response says leads (topic, partition). Throws if that broker id isn't in the brokers list — same "incomplete view" situation JOURNAL.md's epoch tie-break bug was about. */
        public BrokerInfo leaderOf(String topic, int partition) {
            for (TopicMetadata t : topics) {
                if (!t.name().equals(topic)) {
                    continue;
                }
                for (PartitionMetadata p : t.partitions()) {
                    if (p.partitionIndex() == partition) {
                        return brokers.stream().filter(b -> b.nodeId() == p.leaderId()).findFirst()
                                .orElseThrow(() -> new IllegalStateException("leader id " + p.leaderId() + " not present in brokers list"));
                    }
                }
            }
            throw new IllegalStateException("no metadata for " + topic + "-" + partition);
        }

        /** Convenience: how many replicas this response's leader currently believes are in-sync for (topic, partition) — a chaos test's cue that replication has actually caught up at least once, not just that the broker is listening. */
        public int isrSizeOf(String topic, int partition) {
            for (TopicMetadata t : topics) {
                if (!t.name().equals(topic)) {
                    continue;
                }
                for (PartitionMetadata p : t.partitions()) {
                    if (p.partitionIndex() == partition) {
                        return p.isrNodes().size();
                    }
                }
            }
            throw new IllegalStateException("no metadata for " + topic + "-" + partition);
        }
    }

    public MetadataResult metadata(List<String> topics) throws IOException {
        ProtocolWriter w = header(ApiKey.METADATA, (short) 4);
        w.writeArray(topics, ProtocolWriter::writeString);
        w.writeBoolean(false);
        sendFrame(w.toByteArray());

        ProtocolReader r = new ProtocolReader(readFrame());
        r.readInt32(); // correlation_id
        r.readInt32(); // throttle_time_ms
        List<BrokerInfo> brokers = r.readArray(br -> {
            int nodeId = br.readInt32();
            String host = br.readString();
            int port = br.readInt32();
            br.readNullableString(); // rack
            return new BrokerInfo(nodeId, host, port);
        });
        r.readNullableString(); // cluster_id
        r.readInt32(); // controller_id
        List<TopicMetadata> topicMetadata = r.readArray(tr -> {
            tr.readInt16(); // topic error_code
            String name = tr.readString();
            tr.readBoolean(); // is_internal
            List<PartitionMetadata> partitions = tr.readArray(pr -> {
                pr.readInt16(); // partition error_code
                int partitionIndex = pr.readInt32();
                int leaderId = pr.readInt32();
                pr.readArray(ProtocolReader::readInt32); // replica_nodes
                List<Integer> isrNodes = pr.readArray(ProtocolReader::readInt32);
                return new PartitionMetadata(partitionIndex, leaderId, isrNodes);
            });
            return new TopicMetadata(name, partitions);
        });
        return new MetadataResult(brokers, topicMetadata);
    }

    public record ProduceResult(short errorCode, long baseOffset) {}

    public ProduceResult produce(String topic, int partition, byte[] batch, short acks, int timeoutMs) throws IOException {
        ProtocolWriter w = header(ApiKey.PRODUCE, (short) 3);
        w.writeNullableString(null); // transactional_id
        w.writeInt16(acks);
        w.writeInt32(timeoutMs);
        w.writeArray(List.of(topic), (tw, name) -> {
            tw.writeString(name);
            tw.writeArray(List.of(partition), (pw, idx) -> {
                pw.writeInt32(idx);
                pw.writeNullableBytes(batch);
            });
        });
        sendFrame(w.toByteArray());
        if (acks == 0) {
            return new ProduceResult((short) 0, -1); // acks=0: no response is ever sent, PRD §5.3
        }

        ProtocolReader r = new ProtocolReader(readFrame());
        r.readInt32(); // correlation_id
        ProduceResult result = r.readArray(tr -> {
            tr.readString();
            return tr.readArray(pr -> {
                pr.readInt32(); // partition_index
                short errorCode = pr.readInt16();
                long baseOffset = pr.readInt64();
                pr.readInt64(); // log_append_time
                return new ProduceResult(errorCode, baseOffset);
            }).get(0);
        }).get(0);
        r.readInt32(); // throttle_time_ms
        return result;
    }

    public record FetchResult(short errorCode, long highWatermark, byte[] records) {}

    public FetchResult fetch(String topic, int partition, long fetchOffset, int maxWaitMs, int maxBytes) throws IOException {
        ProtocolWriter w = header(ApiKey.FETCH, (short) 4);
        w.writeInt32(-1); // replica_id: -1 = ordinary consumer, not a replica
        w.writeInt32(maxWaitMs);
        w.writeInt32(0); // min_bytes
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
        sendFrame(w.toByteArray());

        ProtocolReader r = new ProtocolReader(readFrame());
        r.readInt32(); // correlation_id
        r.readInt32(); // throttle_time_ms
        return r.readArray(tr -> {
            tr.readString();
            return tr.readArray(pr -> {
                pr.readInt32(); // partition_index
                short errorCode = pr.readInt16();
                long highWatermark = pr.readInt64();
                pr.readInt64(); // last_stable_offset
                pr.readArray(x -> null); // aborted_transactions
                byte[] records = pr.readNullableBytes();
                return new FetchResult(errorCode, highWatermark, records == null ? new byte[0] : records);
            }).get(0);
        }).get(0);
    }

    private ProtocolWriter header(ApiKey apiKey, short version) {
        ProtocolWriter w = new ProtocolWriter();
        w.writeInt16((short) apiKey.key);
        w.writeInt16(version);
        w.writeInt32(correlationId++);
        w.writeNullableString("m5-testsupport-client");
        return w;
    }

    private void sendFrame(byte[] body) throws IOException {
        out.writeInt(body.length);
        out.write(body);
        out.flush();
    }

    private byte[] readFrame() throws IOException {
        int length = in.readInt();
        byte[] body = new byte[length];
        in.readFully(body);
        return body;
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
