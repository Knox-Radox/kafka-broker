package com.advaith.broker.replication;

import com.advaith.broker.protocol.ApiKey;
import com.advaith.broker.protocol.HeaderVersions;
import com.advaith.broker.protocol.ProtocolReader;
import com.advaith.broker.protocol.ProtocolWriter;
import com.advaith.broker.protocol.RequestHeader;
import com.advaith.broker.protocol.ResponseHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * One background thread per peer broker (PRD §8.2), continuously issuing
 * {@code Fetch} requests over one persistent, ordinary client connection —
 * "ordinary" because it uses the exact same wire protocol and codec
 * (ProtocolReader/Writer, RequestHeader/ResponseHeader) a real client
 * would, just with {@code replica_id} set to this broker's own id instead
 * of -1.
 *
 * <p><b>Design deviation from PRD §8.3's suggested mechanism, made
 * deliberately:</b> the PRD text describes reusing the same bounded-
 * {@code select()} ticker §7.3 built for Fetch long-polling. That works
 * beautifully for timers driven by OUR OWN state (Fetch long-polling
 * itself, group heartbeats, acks=-1 completion below) — but a replica
 * fetch is a blocking round trip to ANOTHER PROCESS over the network. Run
 * from inside {@code tick()} on the single selector thread, a slow or
 * dead peer would stall every client's Produce/Fetch for as long as that
 * peer takes to time out — turning one broker's outage into every other
 * broker's latency problem. A small, FIXED number of background threads
 * (bounded by cluster size, never by client count — this is the one
 * meaningful difference from "why not a thread per client", which really
 * would break the single-threaded design's whole point) keeps that
 * failure mode local to this one peer link, at the cost of the one piece
 * of real cross-thread synchronization this project has: see
 * {@link PartitionReplicaState}'s javadoc for the lock discipline.
 *
 * <p><b>Every partition shared with this peer is fetched every round,
 * regardless of who we currently believe leads it</b> — not just the ones
 * where we think the peer is leader. This is what makes epoch fencing
 * (PRD §8.5) work without inventing a second message type: the request we
 * send carries OUR OWN (epoch, believedLeaderId, epochStartOffset) per
 * partition, and the response carries the peer's; whichever side turns
 * out to be behind adopts the other's belief AND immediately truncates
 * any of its own log past that leader's epochStartOffset (see
 * {@link PartitionReplicaState#reconcile}'s javadoc for why raw endpoint
 * comparison alone isn't enough to catch every divergence shape). A
 * reconnected, still-self-believing old leader gets fenced — and its
 * diverged tail discarded — the very next time either direction of this
 * exchange happens; it doesn't need anyone to specifically notify it.
 */
final class PeerReplicator {

    private static final Logger log = LoggerFactory.getLogger(PeerReplicator.class);

    private static final int CONNECT_TIMEOUT_MS = 2000;
    private static final int PARTITION_MAX_BYTES = 1024 * 1024;
    private static final int REQUEST_MAX_BYTES = 8 * 1024 * 1024;
    private static final long RETRY_BACKOFF_MS = 1000;

    private final int selfBrokerId;
    private final PeerInfo peer;
    private final ReplicaManager replicaManager;
    private final ReplicationConfig config;

    private volatile boolean running = true;
    private volatile long lastSuccessfulContactMillis = 0;
    private int nextCorrelationId = 0;

    private Thread thread;
    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;

    PeerReplicator(int selfBrokerId, PeerInfo peer, ReplicaManager replicaManager, ReplicationConfig config) {
        this.selfBrokerId = selfBrokerId;
        this.peer = peer;
        this.replicaManager = replicaManager;
        this.config = config;
    }

    int peerBrokerId() {
        return peer.brokerId();
    }

    boolean isRecentlyReachable(long withinMs) {
        return System.currentTimeMillis() - lastSuccessfulContactMillis <= withinMs;
    }

    void start() {
        thread = new Thread(this::run, "replica-fetcher-to-broker-" + peer.brokerId());
        thread.setDaemon(true);
        thread.start();
    }

    void shutdown() {
        running = false;
        if (thread != null) {
            thread.interrupt();
        }
        closeQuietly();
    }

    private void run() {
        while (running) {
            try {
                List<TopicPartition> shared = replicaManager.partitionsSharedWith(peer.brokerId());
                if (shared.isEmpty()) {
                    Thread.sleep(config.leaderLeaseRenewIntervalMs());
                } else {
                    ensureConnected();
                    doOneFetchRound(shared);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                continue;
            } catch (IOException e) {
                log.debug("replication link to broker {} ({}:{}) failed: {}", peer.brokerId(), peer.host(), peer.port(), e.toString());
                closeQuietly();
                sleepQuietly(RETRY_BACKOFF_MS);
            } catch (RuntimeException e) {
                log.warn("unexpected error in replication link to broker {}", peer.brokerId(), e);
                closeQuietly();
                sleepQuietly(RETRY_BACKOFF_MS);
            }
            replicaManager.checkForPromotions();
        }
        closeQuietly();
    }

    private void doOneFetchRound(List<TopicPartition> partitions) throws IOException {
        int correlationId = ++nextCorrelationId;
        Map<TopicPartition, Long> requestedFetchOffsets = sendFetchRequest(correlationId, partitions);
        ProtocolReader response = readResponse(correlationId);
        applyResponse(response, requestedFetchOffsets);
        lastSuccessfulContactMillis = System.currentTimeMillis();
    }

    /**
     * Sends the request and returns exactly what {@code fetchOffset} was
     * sent for each partition — needed later by {@link #applyResponse} to
     * detect a stale round (see JOURNAL.md, 2026-09-12): if our own log
     * end has moved by the time the response comes back (a truncation
     * triggered by this round's own reconciliation, or — the rarer, still
     * real case this specifically guards against — one triggered by a
     * DIFFERENT peer's round that raced ahead of this one), the records
     * in this response were fetched from a position that's no longer
     * where our log actually ends, and must not be appended blindly.
     */
    private Map<TopicPartition, Long> sendFetchRequest(int correlationId, List<TopicPartition> partitions) throws IOException {
        short apiVersion = ApiKey.FETCH.maxVersion;
        short headerVersion = HeaderVersions.lookup(ApiKey.FETCH.key, apiVersion).requestHeaderVersion();
        RequestHeader header = new RequestHeader((short) ApiKey.FETCH.key, apiVersion, correlationId, "replica-fetcher-" + selfBrokerId);

        ProtocolWriter full = new ProtocolWriter();
        header.write(full, headerVersion);

        full.writeInt32(selfBrokerId); // replica_id: what tells the leader's FetchHandler this is inter-broker traffic, not a real consumer
        full.writeInt32(config.replicaFetchMaxWaitMs());
        full.writeInt32(config.replicaFetchMinBytes());
        full.writeInt32(REQUEST_MAX_BYTES);
        full.writeInt8((byte) 0); // isolation_level — read_uncommitted, the only mode this project implements

        Map<String, List<TopicPartition>> byTopic = new LinkedHashMap<>();
        for (TopicPartition tp : partitions) {
            byTopic.computeIfAbsent(tp.topic(), t -> new ArrayList<>()).add(tp);
        }
        Map<TopicPartition, Long> requestedFetchOffsets = new java.util.HashMap<>();
        full.writeArray(new ArrayList<>(byTopic.entrySet()), (w, entry) -> {
            w.writeString(entry.getKey());
            w.writeArray(entry.getValue(), (pw, tp) -> {
                ReplicaManager.OutboundSnapshot snapshot = replicaManager.snapshotForRequest(tp);
                requestedFetchOffsets.put(tp, snapshot.fetchOffset());
                pw.writeInt32(tp.partition());
                pw.writeInt64(snapshot.fetchOffset());
                pw.writeInt32(PARTITION_MAX_BYTES);
                // M4 broker-only extension (see class javadoc): a real
                // consumer's Fetch (replica_id == -1) never sends these;
                // FetchHandler only reads them when replica_id != -1.
                pw.writeInt32(snapshot.epoch());
                pw.writeInt32(snapshot.believedLeaderId());
                pw.writeInt64(snapshot.epochStartOffset());
            });
        });

        byte[] framed = full.toByteArray();
        out.writeInt(framed.length);
        out.write(framed);
        out.flush();
        return requestedFetchOffsets;
    }

    private ProtocolReader readResponse(int expectedCorrelationId) throws IOException {
        int length = in.readInt();
        if (length <= 0 || length > 100 * 1024 * 1024) {
            throw new IOException("invalid response frame length " + length + " from broker " + peer.brokerId());
        }
        byte[] body = new byte[length];
        in.readFully(body);

        ProtocolReader reader = new ProtocolReader(body);
        short responseHeaderVersion = HeaderVersions.lookup(ApiKey.FETCH.key, ApiKey.FETCH.maxVersion).responseHeaderVersion();
        ResponseHeader header = ResponseHeader.parse(reader, responseHeaderVersion);
        if (header.correlationId() != expectedCorrelationId) {
            throw new IOException("correlation id mismatch talking to broker " + peer.brokerId()
                    + ": expected " + expectedCorrelationId + ", got " + header.correlationId());
        }
        return reader;
    }

    private record PartitionResult(int index, short errorCode, byte[] records,
                                    int responderEpoch, int responderBelievedLeader, long responderEpochStartOffset) {}

    private void applyResponse(ProtocolReader reader, Map<TopicPartition, Long> requestedFetchOffsets) {
        reader.readInt32(); // throttle_time_ms — never throttled here
        List<Map.Entry<String, List<PartitionResult>>> topics = reader.readArray(r -> {
            String topicName = r.readString();
            List<PartitionResult> partitions = r.readArray(pr -> {
                int index = pr.readInt32();
                short errorCode = pr.readInt16();
                // Repurposed for a replica fetch (see FetchHandler): the
                // responder's raw logEndOffset, not the ISR-capped HWM.
                // We don't need it directly any more (truncation now
                // targets epochStartOffset below, not this endpoint —
                // see JOURNAL.md, 2026-09-12) but still have to read past
                // it to stay aligned with the wire format.
                pr.readInt64();
                pr.readInt64(); // last_stable_offset — duplicate of the field above in this project (see FetchHandler)
                pr.readArray(x -> null); // aborted_transactions — always null (transactions are a non-goal)
                byte[] records = pr.readNullableBytes();
                int responderEpoch = pr.readInt32();
                int responderBelievedLeader = pr.readInt32();
                long responderEpochStartOffset = pr.readInt64();
                return new PartitionResult(index, errorCode, records, responderEpoch, responderBelievedLeader, responderEpochStartOffset);
            });
            return Map.entry(topicName, partitions);
        });

        for (Map.Entry<String, List<PartitionResult>> topicEntry : topics) {
            for (PartitionResult result : topicEntry.getValue()) {
                TopicPartition tp = new TopicPartition(topicEntry.getKey(), result.index());
                long requestedFetchOffset = requestedFetchOffsets.get(tp);
                replicaManager.applyReplicaFetchResult(tp, peer.brokerId(), result.errorCode(),
                        result.responderEpoch(), result.responderBelievedLeader(), result.responderEpochStartOffset(),
                        requestedFetchOffset, result.records());
            }
        }
    }

    private void ensureConnected() throws IOException {
        if (socket != null && socket.isConnected() && !socket.isClosed()) {
            return;
        }
        socket = new Socket();
        socket.connect(new InetSocketAddress(peer.host(), peer.port()), CONNECT_TIMEOUT_MS);
        // Bounds how long one round trip can block this thread — generous
        // margin over the server's own long-poll ceiling (replicaFetchMaxWaitMs)
        // so a healthy but momentarily-quiet leader never trips this by
        // accident; a genuinely dead/partitioned peer still gets noticed
        // promptly relative to leader.lease.timeout.ms.
        socket.setSoTimeout(config.replicaFetchMaxWaitMs() + 3000);
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
    }

    private void closeQuietly() {
        try {
            if (socket != null) {
                socket.close();
            }
        } catch (IOException ignored) {
            // already gone; nothing left to clean up
        }
        socket = null;
        in = null;
        out = null;
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
