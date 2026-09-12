package com.advaith.broker.protocol;

/**
 * Parsed request header (PRD §5.2). api_key/api_version/correlation_id are
 * always encoded identically regardless of header version — only what
 * comes after them (client_id, tag buffer) depends on the version, which
 * resolves the chicken-and-egg problem of not knowing the header version
 * until you've read api_version, a field the header version depends on.
 */
public record RequestHeader(short apiKey, short apiVersion, int correlationId, String clientId) {

    /**
     * The write-side counterpart of {@link #parse}, needed since M4 (PRD
     * §8.2): every broker is now also a CLIENT of its peers (the replica
     * fetch loop), so something on this side of the wire has to build a
     * request header, not just decode one. Mirrors parse()'s exact version
     * rules so a request this broker sends is byte-identical in shape to
     * one a real client would send at the same header version.
     */
    public void write(ProtocolWriter writer, short headerVersion) {
        writer.writeInt16(apiKey);
        writer.writeInt16(apiVersion);
        writer.writeInt32(correlationId);
        if (headerVersion >= 1) {
            writer.writeNullableString(clientId);
        }
        if (headerVersion >= 2) {
            writer.writeEmptyTagBuffer();
        }
    }

    public static RequestHeader parse(ProtocolReader reader) {
        short apiKey = reader.readInt16();
        short apiVersion = reader.readInt16();
        int correlationId = reader.readInt32();

        short headerVersion = HeaderVersions.lookup(apiKey, apiVersion).requestHeaderVersion();

        String clientId = null;
        if (headerVersion >= 1) {
            clientId = reader.readNullableString();
        }
        if (headerVersion >= 2) {
            reader.readTagBuffer();
        }
        return new RequestHeader(apiKey, apiVersion, correlationId, clientId);
    }
}
