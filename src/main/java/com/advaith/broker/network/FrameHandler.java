package com.advaith.broker.network;

/**
 * Boundary between the network layer and everything above it (RequestDecoder,
 * in the architecture diagram). The network layer knows nothing about Kafka's
 * wire protocol beyond "size-prefixed frame" — this keeps the selector loop
 * reusable and testable without a protocol codec existing yet.
 */
public interface FrameHandler {
    void onFrame(RequestFrame frame);
}
