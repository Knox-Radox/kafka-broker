package com.advaith.broker.group;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * One consumer group's membership state — PRD §7.1's "essential eager
 * rebalancing": every membership change (a join, a leave, a missed
 * heartbeat) triggers a full stop-the-world rebalance, no incremental/
 * cooperative reassignment (KIP-429, explicitly out of scope). Owned
 * exclusively by {@link GroupCoordinator}, which is the only thing that
 * mutates it — package-private on purpose, since nothing outside the
 * group package should reach into a group's internals directly.
 */
final class ConsumerGroup {

    enum State {
        /** No members. The state a group starts in, and returns to once its last member leaves. */
        EMPTY,
        /** A rebalance is collecting joiners; {@code roundJoins} is being built up until {@code joinDeadlineMillis} or until every expected straggler has rejoined. */
        PREPARING_REBALANCE,
        /** Every joiner has its JoinGroup response; waiting for all of them to call SyncGroup. */
        AWAITING_SYNC,
        /** Rebalance complete — {@code members} is this generation's final, agreed membership. */
        STABLE
    }

    static final class Member {
        final String memberId;
        int sessionTimeoutMs;
        List<ProtocolMetadata> protocols;
        long lastHeartbeatMillis;

        /** Set while this member is waiting on a JoinGroup response this round; null once answered. */
        ResponseSink<JoinGroupResult> joinContext;
        /** Set while this member is waiting on a SyncGroup response this generation; null once answered. */
        ResponseSink<SyncGroupResult> syncContext;
        /** This member's slice of the assignment, once SyncGroup has completed for the current generation. */
        byte[] assignment = new byte[0];

        Member(String memberId) {
            this.memberId = memberId;
        }
    }

    final String groupId;
    State state = State.EMPTY;
    int generationId = 0;
    String protocolType;
    String protocolName;
    String leaderId;

    /** This generation's final membership once STABLE (or the previous generation's, while a new round is being collected). */
    final Map<String, Member> members = new LinkedHashMap<>();

    /** This round's joiners so far, in join order — insertion order IS the leader-election rule (first joiner leads). Cleared into {@code members} at finalize. */
    final Map<String, Member> roundJoins = new LinkedHashMap<>();

    /**
     * Every member id this group has ever handed out, kept forever (not
     * cleared per round) — this is what lets a straggler that already has
     * a memberId from a previous generation be recognized as "known,
     * please rejoin" rather than "brand new, please wait for friends" when
     * it calls JoinGroup again after a rebalance signal (PRD §7.4's
     * JoinGroup semantics: only an empty member_id means "assign me one").
     */
    final Set<String> knownMemberIds = new HashSet<>();

    /**
     * A snapshot of {@code members}' keys taken the instant the CURRENT
     * round started (before any mutation) — who the round is waiting to
     * see rejoin. Once {@code roundJoins} contains all of these, the round
     * finalizes immediately instead of waiting out the full deadline; empty
     * for a brand-new group's very first round, which deliberately has
     * nothing to wait for except the clock (see GroupCoordinator).
     */
    final Set<String> expectedMemberIds = new LinkedHashSet<>();

    /** When PREPARING_REBALANCE should finalize if it hasn't already (see GroupCoordinator#finalizeJoin). -1 = not currently timed. */
    long joinDeadlineMillis = -1;
    long roundStartedMillis = -1;

    ConsumerGroup(String groupId) {
        this.groupId = groupId;
    }
}
