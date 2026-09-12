package com.advaith.broker;

import com.advaith.broker.api.ApiVersionsHandler;
import com.advaith.broker.api.RequestDispatcher;
import com.advaith.broker.metrics.Metrics;
import com.advaith.broker.network.NetworkServer;
import com.advaith.broker.protocol.ApiKey;
import com.advaith.broker.protocol.ProtocolReader;
import com.advaith.broker.protocol.ProtocolWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Drives the whole M1-so-far stack (NetworkServer -> RequestDispatcher ->
 * ApiVersionsHandler) with hand-built wire bytes over a real socket — no
 * shortcuts through the classes under test. This is the same handshake a
 * real `kafka-broker-api-versions.sh` performs (acceptance criterion 1),
 * just with the request/response bytes constructed and checked by hand
 * before trusting a real client against it.
 */
class BrokerHandshakeIntegrationTest {

    private static final int PORT = 28094;

    private NetworkServer server;
    private Thread serverThread;

    @AfterEach
    void tearDown() throws InterruptedException {
        if (server != null) {
            server.stop();
            serverThread.join(2000);
        }
    }

    @Test
    void apiVersionsHandshakeSucceedsOverARealSocket() throws Exception {
        RequestDispatcher dispatcher = new RequestDispatcher(List.of(new ApiVersionsHandler()), new Metrics());
        server = new NetworkServer(PORT, dispatcher);
        serverThread = new Thread(server, "handshake-test-server");
        serverThread.setDaemon(true);
        serverThread.start();
        Thread.sleep(200);

        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("localhost", PORT), 1000);
            socket.getOutputStream().write(buildApiVersionsRequest(4242));
            socket.getOutputStream().flush();

            DataInputStream in = new DataInputStream(socket.getInputStream());
            int responseSize = in.readInt();
            byte[] responseBytes = new byte[responseSize];
            in.readFully(responseBytes);

            ProtocolReader response = new ProtocolReader(responseBytes);
            // Response header v0 (the ApiVersions special case): just the
            // correlation id, echoed back exactly as we sent it — this is
            // how a client matches a pipelined response to its request.
            assertEquals(4242, response.readInt32(), "correlation_id must be echoed back");

            assertEquals(0, response.readInt16(), "error_code");
            List<Integer> advertisedApiKeys = response.readCompactArray(r -> {
                int apiKey = r.readInt16();
                r.readInt16(); // min_version
                r.readInt16(); // max_version
                r.readTagBuffer();
                return apiKey;
            });
            assertEquals(ApiKey.values().length, advertisedApiKeys.size());
        }
    }

    /** Hand-builds exactly the bytes a real client sends for ApiVersions v3. */
    private static byte[] buildApiVersionsRequest(int correlationId) {
        ProtocolWriter w = new ProtocolWriter();
        // Header v2 (flexible): api_key, api_version, correlation_id, client_id, TAG_BUFFER.
        w.writeInt16((short) ApiKey.API_VERSIONS.key);
        w.writeInt16((short) 3);
        w.writeInt32(correlationId);
        w.writeNullableString("integration-test-client");
        w.writeEmptyTagBuffer();
        // Body v3: client_software_name, client_software_version, TAG_BUFFER.
        w.writeCompactString("integration-test");
        w.writeCompactString("0.1");
        w.writeEmptyTagBuffer();

        byte[] body = w.toByteArray();
        ByteBuffer framed = ByteBuffer.allocate(4 + body.length);
        framed.putInt(body.length);
        framed.put(body);
        return framed.array();
    }
}
