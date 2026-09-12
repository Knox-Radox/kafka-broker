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

    /**
     * The read-side counterpart of {@link #write}, needed since M4 (PRD
     * §8.2): the replica fetch loop is this broker acting as a client of a
     * peer, so it has to parse the peer's response headers back, the same
     * way a real consumer would parse ours.
     */
    public static ResponseHeader parse(ProtocolReader reader, short responseHeaderVersion) {
        int correlationId = reader.readInt32();
        if (responseHeaderVersion >= 1) {
            reader.readTagBuffer();
        }
        return new ResponseHeader(correlationId);
    }
}
