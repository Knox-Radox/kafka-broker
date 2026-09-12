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
 *
 * Since M3 (PRD §7.3), {@code select()} no longer always blocks forever:
 * an optional {@link SelectorTicker} can bound its timeout and gets a
 * {@code tick()} every iteration, which is what lets a parked Fetch
 * request's timeout fire even though nothing arrived on any socket.
 */
public final class NetworkServer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(NetworkServer.class);

    private final int port;
    private final FrameHandler frameHandler;
    private final SelectorTicker ticker;
    private volatile boolean running = true;

    private Selector selector;
    private ServerSocketChannel serverChannel;

    public NetworkServer(int port, FrameHandler frameHandler) {
        this(port, frameHandler, SelectorTicker.NONE);
    }

    public NetworkServer(int port, FrameHandler frameHandler, SelectorTicker ticker) {
        this.port = port;
        this.frameHandler = frameHandler;
        this.ticker = ticker;
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
                // Blocks here until at least one channel is ready, a
                // registered interest changes wakes it, OR (since M3) the
                // ticker's own deadline arrives — a bounded timeout instead
                // of an indefinite one is the one, minimal change needed to
                // let the loop notice time passing without a second thread.
                // Still zero busy-polling: with nothing pending, the ticker
                // reports -1 and this blocks exactly as it always did.
                long timeout = ticker.millisUntilNextDeadline();
                if (timeout < 0) {
                    selector.select();
                } else {
                    selector.select(Math.max(1, timeout));
                }
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

            // Runs every iteration regardless of what woke select() up —
            // a produce that just satisfied a parked fetch already
            // completed it above (via the handler path), so most ticks
            // find nothing to do; this is specifically what catches a
            // parked fetch whose max_wait_ms elapsed with no I/O at all.
            try {
                ticker.tick();
            } catch (RuntimeException e) {
                // Same defence-in-depth principle as the per-key handling
                // above: a bug in whatever's ticking (Fetch long-polling,
                // consumer-group rebalance timers) must not kill the whole
                // selector loop.
                log.warn("unexpected error in selector ticker", e);
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
