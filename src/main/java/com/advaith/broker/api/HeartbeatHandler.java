package com.advaith.broker.api;

import com.advaith.broker.group.GroupCoordinator;
import com.advaith.broker.protocol.ApiKey;
import com.advaith.broker.protocol.ProtocolReader;
import com.advaith.broker.protocol.ProtocolWriter;

/**
 * A member's keep-alive (PRD §7.4). Purely a liveness check plus a signal
 * channel: {@code REBALANCE_IN_PROGRESS} here is how a member finds out a
 * rebalance is already underway without it (missed one itself, or another
 * member triggered one) and needs to call JoinGroup again. Unlike
 * JoinGroup/SyncGroup, this never needs to wait on anything else, so it
 * answers synchronously.
 */
public final class HeartbeatHandler implements ApiHandler {

    private final GroupCoordinator groupCoordinator;

    public HeartbeatHandler(GroupCoordinator groupCoordinator) {
        this.groupCoordinator = groupCoordinator;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.HEARTBEAT;
    }

    @Override
    public byte[] handle(RequestContext context, ProtocolReader request) {
        String groupId = request.readString();
        int generationId = request.readInt32();
        String memberId = request.readString();

        short errorCode = groupCoordinator.heartbeat(groupId, generationId, memberId);

        ProtocolWriter response = new ProtocolWriter();
        response.writeInt16(errorCode);
        return response.toByteArray();
    }
}
