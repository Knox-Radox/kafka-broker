package com.advaith.broker.api;

import com.advaith.broker.group.GroupCoordinator;
import com.advaith.broker.group.JoinGroupResult;
import com.advaith.broker.group.ProtocolMetadata;
import com.advaith.broker.protocol.ApiKey;
import com.advaith.broker.protocol.ProtocolReader;
import com.advaith.broker.protocol.ProtocolWriter;

import java.util.List;

/**
 * A member announces itself to a group (PRD §7.4). Unlike every handler
 * before Fetch's long-polling (§7.3), this one is NEVER answered
 * synchronously, even on an immediate validation error — everything goes
 * through {@link GroupCoordinator#joinGroup}'s {@code ResponseSink}, so
 * this handler has exactly one response mechanism to reason about instead
 * of two.
 */
public final class JoinGroupHandler implements ApiHandler {

    private final GroupCoordinator groupCoordinator;

    public JoinGroupHandler(GroupCoordinator groupCoordinator) {
        this.groupCoordinator = groupCoordinator;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.JOIN_GROUP;
    }

    @Override
    public byte[] handle(RequestContext context, ProtocolReader request) {
        String groupId = request.readString();
        int sessionTimeoutMs = request.readInt32();
        int rebalanceTimeoutMs = request.readInt32();
        String memberId = request.readString(); // "" is the sentinel for "please assign me one" (PRD §7.4)
        String protocolType = request.readString();
        List<ProtocolMetadata> protocols = request.readArray(r -> {
            String name = r.readString();
            byte[] metadata = r.readNullableBytes();
            return new ProtocolMetadata(name, metadata == null ? new byte[0] : metadata);
        });

        groupCoordinator.joinGroup(groupId, memberId, protocolType, protocols, sessionTimeoutMs, rebalanceTimeoutMs,
                result -> context.sendAsync(encode(result)));
        return null;
    }

    private static byte[] encode(JoinGroupResult result) {
        ProtocolWriter w = new ProtocolWriter();
        w.writeInt16(result.errorCode());
        w.writeInt32(result.generationId());
        w.writeString(result.protocolName());
        w.writeString(result.leaderId());
        w.writeString(result.memberId());
        // Non-empty only for the elected leader (PRD §7.1) — everyone else
        // gets an empty array, which is exactly how a real client tells the
        // two roles apart (memberId.equals(leaderId)).
        w.writeArray(result.members(), (mw, member) -> {
            mw.writeString(member.memberId());
            mw.writeNullableBytes(member.metadata());
        });
        return w.toByteArray();
    }
}
