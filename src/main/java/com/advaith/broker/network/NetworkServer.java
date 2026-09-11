package com.advaith.broker.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

/**
 * The entire network layer: one thread, one Selector, non-blocking channels.
 * Design decision: single-threaded event loop per PRD §3/§5.8 gate question
 * 15 — no request is ever handled concurrently with another, which is what
 * makes it safe for everything above this layer (codec, handlers, log) to
 * skip synchronization in M1. The cost is a throughput ceiling worth
 * measuring, not assuming.
 */
public final class NetworkServer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(NetworkServer.class);

    private final int port;
    private final FrameHandler frameHandler;
    private volatile boolean running = true;

    private Selector selector;
    private ServerSocketChannel serverChannel;

    public NetworkServer(int port, FrameHandler frameHandler) {
        this.port = port;
        this.frameHandler = frameHandler;
    }

    /** Binds and starts the loop. Blocks the calling thread until stop() is called. */
    @Override
    public void run() {
        try {
            selector = Selector.open();
            serverChannel = ServerSocketChannel.open();
            serverChannel.bind(new InetSocketAddress(port));
            // Non-blocking is what makes registering with a Selector legal
            // at all — a blocking channel can't be multiplexed.
            serverChannel.configureBlocking(false);
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);
            log.info("listening on port {}", port);
        } catch (IOException e) {
            throw new RuntimeException("failed to start network server", e);
        }

        while (running) {
            try {
                // Blocks here until at least one channel is ready, or a
                // registered interest changes wake it. This is *the* line
                // that makes one thread sufficient: no busy-polling.
                selector.select();
            } catch (IOException e) {
                log.error("selector.select() failed, shutting down", e);
                break;
            }

            Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
            while (keys.hasNext()) {
                SelectionKey key = keys.next();
                keys.remove(); // select() does not clear this set itself

                try {
                    if (!key.isValid()) {
                        continue;
                    }
                    if (key.isAcceptable()) {
                        accept();
                    } else if (key.isReadable()) {
                        ((Connection) key.attachment()).handleReadable();
                    } else if (key.isWritable()) {
                        ((Connection) key.attachment()).handleWritable();
                    }
                } catch (IOException e) {
                    // A single bad/disconnected client must not take the
                    // loop down — everyone else's traffic still needs
                    // servicing. Log at debug: a client disconnecting or
                    // sending garbage is routine, not an operator alert.
                    log.debug("closing connection due to: {}", e.toString());
                    closeQuietly(key);
                } catch (RuntimeException e) {
                    // Defence in depth for acceptance criterion 6 / the
                    // fuzz test in §5.7: nothing thrown while handling one
                    // connection may escape the loop and kill the broker.
                    log.warn("unexpected error handling connection, closing it", e);
                    closeQuietly(key);
                }
            }
        }

        closeAll();
    }

    private void accept() throws IOException {
        SocketChannel channel = serverChannel.accept();
        if (channel == null) {
            return; // another thread could have raced us to it; none does in M1, but harmless
        }
        channel.configureBlocking(false);
        SelectionKey key = channel.register(selector, SelectionKey.OP_READ);
        Connection connection = new Connection(channel, key, frameHandler);
        key.attach(connection);
        log.debug("accepted connection from {}", connection.remoteAddress());
    }

    private void closeQuietly(SelectionKey key) {
        Object attachment = key.attachment();
        if (attachment instanceof Connection connection) {
            connection.close();
        } else {
            try {
                key.cancel();
                key.channel().close();
            } catch (IOException | CancelledKeyException ignored) {
                // already gone; nothing left to clean up
            }
        }
    }

    private void closeAll() {
        for (SelectionKey key : selector.keys()) {
            closeQuietly(key);
        }
        try {
            selector.close();
            serverChannel.close();
        } catch (IOException e) {
            log.debug("error during shutdown", e);
        }
    }

    /** Signals the loop to stop and wakes it if it's blocked in select(). */
    public void stop() {
        running = false;
        if (selector != null) {
            selector.wakeup();
        }
    }
}
