package com.advaith.broker.protocol;

/**
 * Response header (PRD §5.2): v0 is just the correlation ID; v1 adds an
 * (always-empty, for us) tag buffer. Which version to write is looked up
 * the same way as the request — see HeaderVersions for the ApiVersions
 * exception in particular.
 */
public record ResponseHeader(int correlationId) {

    public void write(ProtocolWriter writer, short responseHeaderVersion) {
        writer.writeInt32(correlationId);
        if (responseHeaderVersion >= 1) {
            writer.writeEmptyTagBuffer();
        }
    }
}
