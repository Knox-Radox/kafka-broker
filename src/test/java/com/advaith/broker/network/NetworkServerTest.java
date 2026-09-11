package com.advaith.broker.network;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the network layer directly against acceptance criteria 6 and 7
 * (PRD §5.6): partial reads must reassemble correctly, and malformed frames
 * must not take the broker down. No protocol codec involved — the frame
 * payload here is arbitrary bytes, because framing doesn't care what's
 * inside a frame.
 */
class NetworkServerTest {

    private NetworkServer server;
    private Thread serverThread;

    private NetworkServer start(FrameHandler handler) throws InterruptedException {
        // Port 0 would let the OS pick a free port, but NetworkServer
        // doesn't expose the bound port back out yet (no caller has needed
        // it before now), so tests use a fixed high port instead.
        int port = 28093;
        server = new NetworkServer(port, handler);
        serverThread = new Thread(server, "test-network-server");
        serverThread.setDaemon(true);
        serverThread.start();
        Thread.sleep(200); // give the selector loop time to bind before we connect
        return server;
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (server != null) {
            server.stop();
            serverThread.join(2000);
        }
    }

    @Test
    void reassemblesAFrameSplitAcrossMultipleWrites() throws Exception {
        List<byte[]> received = new CopyOnWriteArrayList<>();
        start(frame -> received.add(frame.payload()));

        byte[] body = "hello kafka".getBytes();
        ByteBuffer framed = ByteBuffer.allocate(4 + body.length);
        framed.putInt(body.length);
        framed.put(body);
        framed.flip();
        byte[] wire = new byte[framed.remaining()];
        framed.get(wire);

        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("localhost", 28093), 1000);
            // Deliberately write one byte at a time, forcing the server to
            // see the frame across many separate TCP segments/reads instead
            // of one clean read() that happens to contain everything.
            for (byte b : wire) {
                socket.getOutputStream().write(b);
                socket.getOutputStream().flush();
            }
            waitUntil(() -> received.size() == 1);
        }

        assertEquals(1, received.size());
        assertEquals("hello kafka", new String(received.get(0)));
    }

    @Test
    void multiplePipelinedFramesInOneWriteAreAllDelivered() throws Exception {
        List<byte[]> received = new CopyOnWriteArrayList<>();
        start(frame -> received.add(frame.payload()));

        byte[] wire = concatFrames("first", "second", "third");

        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("localhost", 28093), 1000);
            socket.getOutputStream().write(wire);
            socket.getOutputStream().flush();
            waitUntil(() -> received.size() == 3);
        }

        assertEquals(List.of("first", "second", "third"),
                received.stream().map(String::new).toList());
    }

    @Test
    void malformedLengthClosesOnlyThatConnectionWithoutKillingTheServer() throws Exception {
        List<byte[]> received = new CopyOnWriteArrayList<>();
        start(frame -> received.add(frame.payload()));

        try (Socket bad = new Socket()) {
            bad.connect(new InetSocketAddress("localhost", 28093), 1000);
            ByteBuffer garbage = ByteBuffer.allocate(4);
            garbage.putInt(-1); // negative length: never valid
            garbage.flip();
            byte[] wire = new byte[4];
            garbage.get(wire);
            bad.getOutputStream().write(wire);
            bad.getOutputStream().flush();

            // The server should close its end. Give it time to notice and
            // react, then confirm the connection died from the client's view.
            Thread.sleep(300);
            bad.setSoTimeout(1000);
            int result = bad.getInputStream().read();
            assertEquals(-1, result, "server should have closed the connection");
        }

        // Server must still be alive and able to serve a well-behaved client.
        byte[] wire = concatFrames("still alive");
        try (Socket good = new Socket()) {
            good.connect(new InetSocketAddress("localhost", 28093), 1000);
            good.getOutputStream().write(wire);
            good.getOutputStream().flush();
            waitUntil(() -> received.size() == 1);
        }
        assertEquals("still alive", new String(received.get(0)));
    }

    /**
     * PRD §5.7's "property-ish" test: random byte garbage must never escape
     * the selector loop as an unhandled exception, no matter how it happens
     * to be shaped. This is distinct from the malformed-length test above —
     * that one targets one specific, known-bad length prefix; this one
     * targets arbitrary bytes we never anticipated the exact shape of.
     */
    @Test
    void randomGarbageNeverCrashesTheSelectorLoop() throws Exception {
        List<byte[]> received = new CopyOnWriteArrayList<>();
        start(frame -> received.add(frame.payload()));
        Random random = new Random(42); // fixed seed: a failure here should be reproducible

        for (int i = 0; i < 50; i++) {
            byte[] garbage = new byte[random.nextInt(200)];
            random.nextBytes(garbage);
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress("localhost", 28093), 1000);
                socket.getOutputStream().write(garbage);
                socket.getOutputStream().flush();
            } catch (IOException ignored) {
                // the server may close the connection out from under us mid-write
                // (e.g. it decoded a "valid" length from random bytes and is now
                // waiting for a body that never arrives before we close) — from
                // the client's side that's just a broken pipe, not a test failure.
            }
        }

        // The real assertion: the server is still alive and correctly serves
        // a well-behaved client after 50 rounds of garbage.
        byte[] wire = concatFrames("survived the fuzzing");
        try (Socket good = new Socket()) {
            good.connect(new InetSocketAddress("localhost", 28093), 1000);
            good.getOutputStream().write(wire);
            good.getOutputStream().flush();
            waitUntil(() -> received.stream().anyMatch(b -> "survived the fuzzing".equals(new String(b))));
        }
    }

    private static byte[] concatFrames(String... bodies) {
        int total = 0;
        for (String b : bodies) total += 4 + b.getBytes().length;
        ByteBuffer buf = ByteBuffer.allocate(total);
        for (String b : bodies) {
            byte[] bytes = b.getBytes();
            buf.putInt(bytes.length);
            buf.put(bytes);
        }
        buf.flip();
        byte[] out = new byte[buf.remaining()];
        buf.get(out);
        return out;
    }

    private static void waitUntil(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) return;
            Thread.sleep(20);
        }
        assertTrue(condition.getAsBoolean(), "condition not met within timeout");
    }
}
