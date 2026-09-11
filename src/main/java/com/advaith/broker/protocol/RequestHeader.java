package com.advaith.broker.protocol;

/**
 * Parsed request header (PRD §5.2). api_key/api_version/correlation_id are
 * always encoded identically regardless of header version — only what
 * comes after them (client_id, tag buffer) depends on the version, which
 * resolves the chicken-and-egg problem of not knowing the header version
 * until you've read api_version, a field the header version depends on.
 */
public record RequestHeader(short apiKey, short apiVersion, int correlationId, String clientId) {

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
