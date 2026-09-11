package com.advaith.broker.api;

import com.advaith.broker.protocol.ApiKey;
import com.advaith.broker.protocol.Errors;
import com.advaith.broker.protocol.ProtocolReader;
import com.advaith.broker.protocol.ProtocolWriter;

import java.util.List;

/**
 * The client's first call (PRD §5.3): negotiates which API versions the
 * broker supports. Design decision: unlike every other handler, this one
 * must produce a coherent answer even when it does NOT support the
 * requested version, because negotiation hasn't happened yet when this
 * request arrives — see the v0 fallback below.
 */
public final class ApiVersionsHandler implements ApiHandler {

    @Override
    public ApiKey apiKey() {
        return ApiKey.API_VERSIONS;
    }

    @Override
    public byte[] handle(short apiVersion, ProtocolReader request) {
        if (!ApiKey.API_VERSIONS.supportsVersion(apiVersion)) {
            return unsupportedVersionResponseV0();
        }
        if (apiVersion == 3) {
            return v3Response(request);
        }
        // Versions 0-2: this is not a hypothetical — a real client's own
        // recovery path sends exactly this after an unsupported first
        // guess (see JOURNAL.md, 2026-09-11), so it has to actually
        // succeed, not just be "technically in range". Their request body
        // is empty (client_software_name/version were added in v3), so
        // there's nothing to read here — only the response shape changes:
        // v0 predates throttle_time_ms entirely; v1-v2 added it but not
        // yet the compact/tagged-field encoding v3 introduced.
        ProtocolWriter response = new ProtocolWriter();
        response.writeInt16(Errors.NONE);
        response.writeArray(List.of(ApiKey.values()), (w, key) -> {
            w.writeInt16((short) key.key);
            w.writeInt16(key.minVersion);
            w.writeInt16(key.maxVersion);
        });
        if (apiVersion >= 1) {
            response.writeInt32(0); // throttle_time_ms — introduced in v1
        }
        return response.toByteArray();
    }

    private byte[] v3Response(ProtocolReader request) {
        // v3 request body: client_software_name/version as COMPACT_STRING,
        // then a TAG_BUFFER. We don't act on the client's declared software
        // identity in M1, but we still must consume these bytes off the
        // wire to leave the reader in a consistent state (there's nothing
        // after them in this particular request, but every handler should
        // read exactly what its schema says is there, on principle).
        request.readCompactString(); // client_software_name — unused
        request.readCompactString(); // client_software_version — unused
        request.readTagBuffer();

        ProtocolWriter response = new ProtocolWriter();
        response.writeInt16(Errors.NONE);
        response.writeCompactArray(List.of(ApiKey.values()), (w, key) -> {
            w.writeInt16((short) key.key);
            w.writeInt16(key.minVersion);
            w.writeInt16(key.maxVersion);
            w.writeEmptyTagBuffer();
        });
        response.writeInt32(0); // throttle_time_ms — no throttling implemented
        response.writeEmptyTagBuffer();
        return response.toByteArray();
    }

    /**
     * PRD §5.3: on an unsupported version, reply with error_code=35 using
     * the OLDEST response shape (v0) — no throttle_time_ms, a plain
     * (non-compact) ARRAY, no tag buffers anywhere — because we cannot
     * assume a client that guessed our version wrong understands any of
     * the newer encoding conventions. v0 predates all of that, so it's the
     * one shape every real client, however old, can still parse.
     */
    private byte[] unsupportedVersionResponseV0() {
        ProtocolWriter response = new ProtocolWriter();
        response.writeInt16(Errors.UNSUPPORTED_VERSION);
        response.writeArray(List.of(), (w, key) -> {}); // PRD: "empty/limited api_keys list"
        return response.toByteArray();
    }
}
