package com.advaith.broker.group;

import com.advaith.broker.network.SelectorTicker;
import com.advaith.broker.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * The whole consumer-group state machine (PRD §7.1/§7.4/§7.5): membership,
 * rebalancing, generation fencing. One instance for the whole broker, one
 * {@link ConsumerGroup} per group id.
 *
 * Design: this class never blocks and never computes a partition
 * assignment — per PRD §7.1, that's the elected LEADER's job, done
 * entirely client-side; the broker's only role is the join/sync barrier
 * and relaying. Like {@code FetchHandler}'s pending-fetch machinery
 * (§7.3), a JoinGroup or SyncGroup that can't be answered immediately is
 * "parked" (its {@link ResponseSink}, reachable from the same single
 * selector thread) rather than blocking, and completed later either by
 * another member's request arriving or by {@link #tick()} — the exact
 * same "detect who's alive via a deadline" pattern PRD §7.4 explicitly
 * calls out as reused, not reinvented, for heartbeats too.
 */
public final class GroupCoordinator implements SelectorTicker {

    private static final Logger log = LoggerFactory.getLogger(GroupCoordinator.class);

    /** The internal, broker-managed topic consumer offsets are durably stored in (PRD §7.4) — real Kafka's own name for it, kept for familiarity. */
    public static final String OFFSETS_TOPIC = "__consumer_offsets";

    private final GroupConfig config;
    private final Map<String, ConsumerGroup> groups = new HashMap<>();

    public GroupCoordinator(GroupConfig config) {
        this.config = config;
    }

    /**
     * PRD §7.4's FindCoordinator: "implement the real lookup shape... rather
     * than hardcoding 'always me'". With one broker the answer is always
     * this broker regardless of the result, but the computation itself —
     * which internal-topic partition a group's commits/coordination route
     * through — is real and is exactly what M4 needs to already exist once
     * there's more than one broker to route to.
     */
    public static int coordinatorPartitionFor(String groupId, int partitionCount) {
        return Math.floorMod(groupId.hashCode(), partitionCount);
    }

    // ==================== JoinGroup ====================

    public void joinGroup(String groupId, String requestedMemberId, String protocolType,
                          List<ProtocolMetadata> protocols, int sessionTimeoutMs, int rebalanceTimeoutMs,
                          ResponseSink<JoinGroupResult> sink) {
        if (sessionTimeoutMs < config.minSessionTimeoutMs() || sessionTimeoutMs > config.maxSessionTimeoutMs()) {
            sink.send(JoinGroupResult.error(requestedMemberId, Errors.INVALID_SESSION_TIMEOUT));
            return;
        }

        ConsumerGroup group = groups.computeIfAbsent(groupId, ConsumerGroup::new);
        long now = System.currentTimeMillis();

        // Only an empty member_id is the client's "please assign me one"
        // sentinel (PRD §7.4) — anything else must already be a member id
        // we ourselves handed out at some point, or this is a stale/bogus
        // request we should reject rather than silently treat as fine.
        if (!requestedMemberId.isEmpty() && !group.knownMemberIds.contains(requestedMemberId)) {
            sink.send(JoinGroupResult.error(requestedMemberId, Errors.UNKNOWN_MEMBER_ID));
            return;
        }
        String memberId = requestedMemberId.isEmpty() ? generateMemberId(groupId) : requestedMemberId;
        group.knownMemberIds.add(memberId);

        startRebalanceRound(group, now);

        ConsumerGroup.Member member = group.roundJoins.computeIfAbsent(memberId, ConsumerGroup.Member::new);
        member.protocols = protocols;
        member.sessionTimeoutMs = sessionTimeoutMs;
        member.lastHeartbeatMillis = now; // joining is itself proof of life
        member.joinContext = sink;
        group.protocolType = protocolType;

        // Fast path: once every member the round is waiting on has shown
        // back up, there's no reason to sit out the rest of the deadline —
        // this is what makes acceptance criterion 4 (survivor reassigned
        // "within session.timeout.ms", not the much longer rebalance
        // ceiling) actually hold. A brand-new group's very first round has
        // an empty expectedMemberIds by design (nothing to wait for except
        // a possible second joiner), so it always rides out the full
        // initial delay instead.
        if (!group.expectedMemberIds.isEmpty() && group.roundJoins.keySet().containsAll(group.expectedMemberIds)) {
            finalizeJoin(group, now);
        }
    }

    /** Moves a group into PREPARING_REBALANCE if it isn't already, snapshotting who this round expects to see rejoin. Idempotent while already collecting. */
    private void startRebalanceRound(ConsumerGroup group, long now) {
        if (group.state == ConsumerGroup.State.PREPARING_REBALANCE) {
            return; // already collecting this round; joinGroup() just adds to it
        }
        if (group.state == ConsumerGroup.State.AWAITING_SYNC) {
            // A new membership change disrupting an in-flight sync barrier
            // — fail anyone already parked on SyncGroup for the round being
            // abandoned now, rather than leaving them to hang until their
            // own client eventually times out with no explanation.
            for (ConsumerGroup.Member m : group.members.values()) {
                if (m.syncContext != null) {
                    ResponseSink<SyncGroupResult> ctx = m.syncContext;
                    m.syncContext = null;
                    ctx.send(SyncGroupResult.error(Errors.REBALANCE_IN_PROGRESS));
                }
            }
        }

        group.expectedMemberIds.clear();
        group.expectedMemberIds.addAll(group.members.keySet());
        group.roundJoins.clear();
        group.roundStartedMillis = now;
        // A brand-new/just-emptied group has nothing to wait for except a
        // possible second joiner (real Kafka's own group.initial.rebalance.
        // delay.ms exists for exactly this); a group with known survivors
        // to wait for gets the full rebalance-timeout ceiling, since the
        // fast path above almost always finalizes it long before that
        // anyway and a genuine straggler needs real time to notice.
        group.joinDeadlineMillis = now + (group.expectedMemberIds.isEmpty()
                ? config.initialRebalanceDelayMs()
                : config.rebalanceTimeoutCeilingMs());
        group.state = ConsumerGroup.State.PREPARING_REBALANCE;
    }

    /** Called either from the fast path above or from tick() once the deadline passes. */
    private void finalizeJoin(ConsumerGroup group, long now) {
        if (group.roundJoins.isEmpty()) {
            // Triggered by a departure/timeout and nobody ever rejoined —
            // nothing to finalize; let the group settle back to EMPTY.
            group.state = ConsumerGroup.State.EMPTY;
            group.joinDeadlineMillis = -1;
            group.members.clear();
            return;
        }

        Optional<String> protocolName = electProtocol(group.roundJoins.values());
        if (protocolName.isEmpty()) {
            // No protocol name is common to every joiner — there is no
            // coherent way to run this rebalance. Tell everyone and reset,
            // rather than pretending a leader could still compute anything.
            for (ConsumerGroup.Member m : group.roundJoins.values()) {
                ResponseSink<JoinGroupResult> ctx = m.joinContext;
                m.joinContext = null;
                ctx.send(JoinGroupResult.error(m.memberId, Errors.INCONSISTENT_GROUP_PROTOCOL));
            }
            group.state = ConsumerGroup.State.EMPTY;
            group.members.clear();
            group.joinDeadlineMillis = -1;
            return;
        }

        group.generationId++;
        group.members.clear();
        group.members.putAll(group.roundJoins);
        group.roundJoins.clear();
        group.leaderId = group.members.keySet().iterator().next(); // LinkedHashMap: first inserted this round = first to join = leader (PRD §7.4)
        group.protocolName = protocolName.get();
        group.joinDeadlineMillis = -1;
        group.state = ConsumerGroup.State.AWAITING_SYNC;

        List<MemberSubscription> membersForLeader = new ArrayList<>();
        for (ConsumerGroup.Member m : group.members.values()) {
            membersForLeader.add(new MemberSubscription(m.memberId, metadataFor(m, group.protocolName)));
        }

        for (ConsumerGroup.Member m : group.members.values()) {
            boolean isLeader = m.memberId.equals(group.leaderId);
            ResponseSink<JoinGroupResult> ctx = m.joinContext;
            m.joinContext = null;
            ctx.send(new JoinGroupResult(Errors.NONE, group.generationId, group.protocolName, group.leaderId,
                    m.memberId, isLeader ? membersForLeader : List.of()));
        }
    }

    private static byte[] metadataFor(ConsumerGroup.Member member, String protocolName) {
        for (ProtocolMetadata p : member.protocols) {
            if (p.name().equals(protocolName)) {
                return p.metadata();
            }
        }
        // Unreachable given electProtocol() only ever picks a name every
        // joiner actually listed, but never let a lookup miss throw from
        // inside the selector loop's tick() path.
        return new byte[0];
    }

    /**
     * Real Kafka's own algorithm (GroupMetadata#selectProtocol, mirrored
     * here): every member "votes" for the first protocol name in its OWN
     * preference order that every other member also supports; the name
     * with the most votes wins. Empty result means no name is common to
     * every joiner at all — a real, if rare, possibility worth handling
     * rather than assuming away.
     */
    private static Optional<String> electProtocol(Collection<ConsumerGroup.Member> members) {
        Set<String> candidates = null;
        for (ConsumerGroup.Member m : members) {
            Set<String> names = new LinkedHashSet<>();
            for (ProtocolMetadata p : m.protocols) {
                names.add(p.name());
            }
            candidates = (candidates == null) ? names : intersect(candidates, names);
        }
        if (candidates == null || candidates.isEmpty()) {
            return Optional.empty();
        }

        Map<String, Integer> votes = new HashMap<>();
        for (ConsumerGroup.Member m : members) {
            for (ProtocolMetadata p : m.protocols) {
                if (candidates.contains(p.name())) {
                    votes.merge(p.name(), 1, Integer::sum);
                    break; // this member's vote goes to the first supported candidate in ITS OWN order
                }
            }
        }
        return votes.entrySet().stream().max(Map.Entry.comparingByValue()).map(Map.Entry::getKey);
    }

    private static Set<String> intersect(Set<String> a, Set<String> b) {
        Set<String> out = new LinkedHashSet<>(a);
        out.retainAll(b);
        return out;
    }

    private String generateMemberId(String groupId) {
        // Real Kafka's own shape is "<client-id>-<uuid>"; we don't thread
        // client_id down into this layer (it's header-level, and the id is
        // opaque to the client either way), so groupId stands in — purely
        // cosmetic, never parsed by anything.
        return groupId + "-" + UUID.randomUUID();
    }

    // ==================== SyncGroup ====================

    public void syncGroup(String groupId, int generationId, String memberId,
                          List<GroupAssignment> leaderAssignments, ResponseSink<SyncGroupResult> sink) {
        ConsumerGroup group = groups.get(groupId);
        if (group == null || !group.members.containsKey(memberId)) {
            sink.send(SyncGroupResult.error(Errors.UNKNOWN_MEMBER_ID));
            return;
        }
        if (generationId != group.generationId) {
            sink.send(SyncGroupResult.error(Errors.ILLEGAL_GENERATION));
            return;
        }

        ConsumerGroup.Member member = group.members.get(memberId);

        if (group.state == ConsumerGroup.State.STABLE) {
            // Already resolved this generation — most likely a retried
            // SyncGroup after a response got lost. Real Kafka answers this
            // the same way: with what was already decided, not an error.
            sink.send(new SyncGroupResult(Errors.NONE, member.assignment));
            return;
        }
        if (group.state != ConsumerGroup.State.AWAITING_SYNC) {
            sink.send(SyncGroupResult.error(Errors.REBALANCE_IN_PROGRESS));
            return;
        }

        if (memberId.equals(group.leaderId) && leaderAssignments != null) {
            for (GroupAssignment a : leaderAssignments) {
                ConsumerGroup.Member target = group.members.get(a.memberId());
                if (target != null) {
                    target.assignment = a.assignment();
                }
            }
        }

        member.syncContext = sink;
        member.lastHeartbeatMillis = System.currentTimeMillis(); // syncing is proof of life too

        boolean everyoneSynced = true;
        for (ConsumerGroup.Member m : group.members.values()) {
            if (m.syncContext == null) {
                everyoneSynced = false;
                break;
            }
        }
        if (everyoneSynced) {
            for (ConsumerGroup.Member m : group.members.values()) {
                ResponseSink<SyncGroupResult> ctx = m.syncContext;
                m.syncContext = null;
                ctx.send(new SyncGroupResult(Errors.NONE, m.assignment));
            }
            group.state = ConsumerGroup.State.STABLE;
        }
    }

    // ==================== Heartbeat ====================

    public short heartbeat(String groupId, int generationId, String memberId) {
        ConsumerGroup group = groups.get(groupId);
        short fencingError = validateMember(group, memberId, generationId);
        if (fencingError != Errors.NONE) {
            return fencingError;
        }
        group.members.get(memberId).lastHeartbeatMillis = System.currentTimeMillis();
        // Tells the client's own heartbeat thread to call JoinGroup again
        // (PRD §7.4) — the mechanism by which a member finds out a
        // rebalance it didn't ask for is already underway.
        return group.state == ConsumerGroup.State.STABLE ? Errors.NONE : Errors.REBALANCE_IN_PROGRESS;
    }

    /**
     * The same generation-fencing check heartbeat() applies, exposed for
     * OffsetCommit (PRD §7.5 names both explicitly): a committed offset
     * from a member the group has already moved on from is exactly the
     * "thinks it still owns partition 3 after being reassigned" bug
     * generation fencing exists to prevent, so OffsetCommit must reject it
     * the same way rather than writing it through.
     */
    public short checkGeneration(String groupId, int generationId, String memberId) {
        return validateMember(groups.get(groupId), memberId, generationId);
    }

    private static short validateMember(ConsumerGroup group, String memberId, int generationId) {
        if (group == null || !group.members.containsKey(memberId)) {
            return Errors.UNKNOWN_MEMBER_ID;
        }
        if (generationId != group.generationId) {
            return Errors.ILLEGAL_GENERATION;
        }
        return Errors.NONE;
    }

    // ==================== LeaveGroup ====================

    public short leaveGroup(String groupId, String memberId) {
        ConsumerGroup group = groups.get(groupId);
        if (group == null || !group.members.containsKey(memberId)) {
            return Errors.UNKNOWN_MEMBER_ID;
        }
        removeMemberAndTriggerRebalance(group, memberId, System.currentTimeMillis());
        return Errors.NONE;
    }

    private void removeMemberAndTriggerRebalance(ConsumerGroup group, String memberId, long now) {
        group.members.remove(memberId);
        if (group.members.isEmpty()) {
            group.state = ConsumerGroup.State.EMPTY;
            group.joinDeadlineMillis = -1;
            return;
        }
        // Every survivor's own next Heartbeat now gets REBALANCE_IN_PROGRESS
        // (see heartbeat() above), which is what actually prompts each of
        // them to call JoinGroup again — startRebalanceRound() below just
        // opens the window for them to do that in.
        startRebalanceRound(group, now);
    }

    // ==================== SelectorTicker ====================

    @Override
    public long millisUntilNextDeadline() {
        long now = System.currentTimeMillis();
        long soonest = -1;
        for (ConsumerGroup group : groups.values()) {
            Long candidate = null;
            if (group.state == ConsumerGroup.State.PREPARING_REBALANCE) {
                candidate = group.joinDeadlineMillis;
            } else if (group.state == ConsumerGroup.State.STABLE) {
                for (ConsumerGroup.Member m : group.members.values()) {
                    long deadline = m.lastHeartbeatMillis + m.sessionTimeoutMs;
                    candidate = (candidate == null) ? deadline : Math.min(candidate, deadline);
                }
            }
            if (candidate != null) {
                soonest = (soonest < 0) ? candidate : Math.min(soonest, candidate);
            }
        }
        return soonest < 0 ? -1 : Math.max(0, soonest - now);
    }

    @Override
    public void tick() {
        long now = System.currentTimeMillis();
        for (ConsumerGroup group : groups.values()) {
            try {
                tickGroup(group, now);
            } catch (RuntimeException e) {
                // Same defence-in-depth principle as the selector loop
                // itself (PRD acceptance criterion 6) — one group's bug
                // must not take every other group's ticking down with it.
                log.warn("error ticking group {}, leaving its state as-is this round", group.groupId, e);
            }
        }
    }

    private void tickGroup(ConsumerGroup group, long now) {
        if (group.state == ConsumerGroup.State.PREPARING_REBALANCE && now >= group.joinDeadlineMillis) {
            finalizeJoin(group, now);
            return;
        }
        if (group.state != ConsumerGroup.State.STABLE) {
            return; // AWAITING_SYNC has no explicit timeout in M3's scope — see STUDY_GUIDE's honest-limitations note
        }
        for (String memberId : new ArrayList<>(group.members.keySet())) {
            ConsumerGroup.Member m = group.members.get(memberId);
            if (m != null && now - m.lastHeartbeatMillis > m.sessionTimeoutMs) {
                removeMemberAndTriggerRebalance(group, memberId, now);
                return; // group state just changed away from STABLE; a second timed-out member (if any) is picked up on the next tick
            }
        }
    }
}
