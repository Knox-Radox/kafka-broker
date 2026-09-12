package com.advaith.broker.api;

import com.advaith.broker.group.GroupAssignment;
import com.advaith.broker.group.GroupCoordinator;
import com.advaith.broker.group.SyncGroupResult;
import com.advaith.broker.protocol.ApiKey;
import com.advaith.broker.protocol.ProtocolReader;
import com.advaith.broker.protocol.ProtocolWriter;

import java.util.List;

/**
 * Every member calls this after JoinGroup; the LEADER's request alone
 * carries the assignment it computed client-side for everyone (PRD §7.4).
 * The broker's whole job here is the barrier (wait for every current
 * member to call this) plus relaying each member its own slice back —
 * there is no assignment algorithm to run.
 */
public final class SyncGroupHandler implements ApiHandler {

    private final GroupCoordinator groupCoordinator;

    public SyncGroupHandler(GroupCoordinator groupCoordinator) {
        this.groupCoordinator = groupCoordinator;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.SYNC_GROUP;
    }

    @Override
    public byte[] handle(RequestContext context, ProtocolReader request) {
        String groupId = request.readString();
        int generationId = request.readInt32();
        String memberId = request.readString();
        // Non-leaders send an empty array here (PRD §7.4) — they aren't
        // computing anything, just waiting for their own slice back.
        List<GroupAssignment> assignments = request.readArray(r -> {
            String assignedMemberId = r.readString();
            byte[] assignment = r.readNullableBytes();
            return new GroupAssignment(assignedMemberId, assignment == null ? new byte[0] : assignment);
        });

        groupCoordinator.syncGroup(groupId, generationId, memberId, assignments,
                result -> context.sendAsync(encode(result)));
        return null;
    }

    private static byte[] encode(SyncGroupResult result) {
        ProtocolWriter w = new ProtocolWriter();
        w.writeInt16(result.errorCode());
        w.writeNullableBytes(result.assignment());
        return w.toByteArray();
    }
}
