package com.advaith.broker.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Per-socket read/write state machine. This class exists because a single
 * channel.read() gives no guarantee about how much of a frame arrives at
 * once — the state machine is what lets us resume correctly no matter how
 * TCP chops up the bytes (PRD acceptance criterion 7: partial reads).
 *
 * One instance per client socket, attached to its SelectionKey. All methods
 * are called from the single selector thread (NetworkServer's run loop), so
 * there is no internal synchronization — that would be needed the moment
 * any other thread touches a Connection.
 */
public final class Connection {

    private static final Logger log = LoggerFactory.getLogger(Connection.class);

    /**
     * Refuses to allocate a body buffer bigger than this for one frame.
     * Without a cap, a malformed or hostile 4-byte length prefix (say,
     * 0x7FFFFFFF) would make us try to allocate ~2GB before we've verified
     * a single byte of it is a real Kafka request — an easy way to OOM the
     * broker from one bad connection. Kafka itself enforces the equivalent
     * limit via socket.request.max.bytes; the exact number is less important
     * than having *a* bound.
     */
    private static final int MAX_FRAME_BYTES = 100 * 1024 * 1024; // 100 MiB

    private enum ReadState { READING_LENGTH, READING_BODY }

    private final SocketChannel channel;
    private final SelectionKey key;
    private final FrameHandler frameHandler;
    private final String remoteAddress;

    private ReadState readState = ReadState.READING_LENGTH;
    private final ByteBuffer lengthBuf = ByteBuffer.allocate(4);
    private ByteBuffer bodyBuf; // allocated once we know the frame length

    // Responses queued to go out but not yet fully written to the socket.
    // A write() call is not guaranteed to accept everything you hand it
    // (the kernel's send buffer can be full), so anything left over has to
    // wait here for the next OP_WRITE-ready event.
    private final Deque<ByteBuffer> pendingWrites = new ArrayDeque<>();

    Connection(SocketChannel channel, SelectionKey key, FrameHandler frameHandler) {
        this.channel = channel;
        this.key = key;
        this.frameHandler = frameHandler;
        this.remoteAddress = remoteAddressOf(channel);
    }

    public String remoteAddress() {
        return remoteAddress;
    }

    /** Called when the selector reports this channel is readable. */
    void handleReadable() throws IOException {
        while (true) {
            if (readState == ReadState.READING_LENGTH) {
                if (!fill(lengthBuf)) {
                    return; // length prefix still incomplete; wait for more data
                }
                // fill() leaves the buffer position == limit (full). flip()
                // resets position to 0 so getInt() reads from the start of
                // what we just wrote, instead of from the end of it.
                lengthBuf.flip();
                int length = lengthBuf.getInt();
                lengthBuf.clear(); // ready to reuse for the *next* frame's length

                if (length <= 0 || length > MAX_FRAME_BYTES) {
                    throw new IOException("invalid frame length " + length + " from " + remoteAddress);
                }
                bodyBuf = ByteBuffer.allocate(length);
                readState = ReadState.READING_BODY;
                // fall through and try to read the body immediately — the
                // same read() that delivered the length may have also
                // delivered some or all of the body in the same TCP segment.
            }

            if (!fill(bodyBuf)) {
                return; // body still incomplete; wait for more data
            }

            bodyBuf.flip();
            byte[] payload = new byte[bodyBuf.remaining()];
            bodyBuf.get(payload);
            readState = ReadState.READING_LENGTH;
            bodyBuf = null;

            frameHandler.onFrame(new RequestFrame(this, payload));
            // Loop back around: a client may pipeline multiple requests
            // without waiting for a response (correlation IDs are what let
            // it match responses back up), so more than one complete frame
            // can already be sitting in what we just read.
        }
    }

    /**
     * Reads from the channel into buf until either buf is full (returns
     * true) or the channel currently has no more bytes to give us (returns
     * false, buf left partially filled for next time).
     */
    private boolean fill(ByteBuffer buf) throws IOException {
        int read = channel.read(buf);
        if (read == -1) {
            throw new IOException("peer closed connection: " + remoteAddress);
        }
        return !buf.hasRemaining();
    }

    /**
     * Queues a fully-framed response (size prefix already included) for
     * writing. A cancelled key here (see JOURNAL.md, 2026-09-12) means the
     * client on the other end is already gone by the time this got
     * called — routine for a DEFERRED response (a parked Fetch/Produce
     * completed later by a tick or another connection's traffic, since
     * M3/M4), never possible for an immediate one (RequestDispatcher calls
     * this on the same connection whose own read event it's still
     * handling). Nothing to send it to any more — drop it and move on,
     * the same "one bad connection can't take anything else down" spirit
     * as NetworkServer's own per-key exception handling.
     */
    public void enqueueResponse(ByteBuffer framedResponse) {
        try {
            pendingWrites.addLast(framedResponse);
            // Only ask the selector to tell us about write-readiness while
            // we actually have something to write. OP_WRITE is ready
            // almost all the time on an idle socket (the kernel send
            // buffer has room), so leaving it registered permanently turns
            // select() into a busy loop that never blocks — 100% CPU for
            // no work done.
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
        } catch (CancelledKeyException e) {
            log.debug("dropping a deferred response to {}: connection already closed", remoteAddress);
        }
    }

    /** Called when the selector reports this channel is writable. */
    void handleWritable() throws IOException {
        while (!pendingWrites.isEmpty()) {
            ByteBuffer buf = pendingWrites.peekFirst();
            channel.write(buf);
            if (buf.hasRemaining()) {
                return; // kernel send buffer is full; resume next OP_WRITE event
            }
            pendingWrites.pollFirst();
        }
        // Drained the queue: stop asking for OP_WRITE until enqueueResponse
        // adds something new, for the same busy-loop reason as above.
        key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
    }

    public void close() {
        try {
            key.cancel();
            channel.close();
        } catch (IOException | CancelledKeyException e) {
            log.debug("error closing connection to {}: {}", remoteAddress, e.toString());
        }
    }

    private static String remoteAddressOf(SocketChannel channel) {
        try {
            return String.valueOf(channel.getRemoteAddress());
        } catch (IOException e) {
            return "unknown";
        }
    }
}
