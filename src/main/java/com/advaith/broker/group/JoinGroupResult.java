package com.advaith.broker.group;

import java.util.List;

/**
 * What {@link GroupCoordinator#joinGroup} eventually hands back to one
 * member via its {@link ResponseSink}. {@code members} is only ever
 * non-empty for the group's leader (PRD §7.1) — everyone else gets an
 * empty list, which is exactly how a real client tells the two cases
 * apart client-side ({@code memberId.equals(leaderId)}).
 */
public record JoinGroupResult(
        short errorCode,
        int generationId,
        String protocolName,
        String leaderId,
        String memberId,
        List<MemberSubscription> members) {

    static JoinGroupResult error(short errorCode) {
        return new JoinGroupResult(errorCode, -1, "", "", "", List.of());
    }

    public static JoinGroupResult error(String memberId, short errorCode) {
        return new JoinGroupResult(errorCode, -1, "", "", memberId, List.of());
    }
}
