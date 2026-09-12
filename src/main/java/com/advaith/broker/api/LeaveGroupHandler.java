package com.advaith.broker.api;

import com.advaith.broker.group.GroupCoordinator;
import com.advaith.broker.protocol.ApiKey;
import com.advaith.broker.protocol.ProtocolReader;
import com.advaith.broker.protocol.ProtocolWriter;

/**
 * Explicit, voluntary departure (PRD §7.4) — the same effect as a
 * heartbeat timeout (triggers a rebalance for whoever's left) but
 * immediate rather than waited-out.
 */
public final class LeaveGroupHandler implements ApiHandler {

    private final GroupCoordinator groupCoordinator;

    public LeaveGroupHandler(GroupCoordinator groupCoordinator) {
        this.groupCoordinator = groupCoordinator;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.LEAVE_GROUP;
    }

    @Override
    public byte[] handle(RequestContext context, ProtocolReader request) {
        String groupId = request.readString();
        String memberId = request.readString();

        short errorCode = groupCoordinator.leaveGroup(groupId, memberId);

        ProtocolWriter response = new ProtocolWriter();
        response.writeInt16(errorCode);
        return response.toByteArray();
    }
}
