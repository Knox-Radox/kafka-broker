package com.advaith.broker.group;

import com.advaith.broker.protocol.Errors;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the join/sync barrier and generation fencing directly against
 * {@link GroupCoordinator} — no network involved, per PRD §7.8's unit-test
 * list: "a JoinGroup before all expected members arrive must not resolve
 * early; once they have, all pending JoinGroup responses resolve together
 * with a consistent generation," plus "a request one generation behind is
 * rejected; one at the current generation succeeds."
 */
class GroupCoordinatorTest {

    // A short initial delay so tests that need the deadline to actually
    // elapse (via a real Thread.sleep) stay fast, without needing to make
    // GroupCoordinator's clock injectable just for testing.
    private static final GroupConfig CONFIG = new GroupConfig(0, 300_000, 60_000, 50);

    private static ProtocolMetadata protocol(String name) {
        return new ProtocolMetadata(name, ("meta-" + name).getBytes());
    }

    @Test
    void singleJoinDoesNotResolveBeforeTheInitialDelayWindowElapses() {
        GroupCoordinator coordinator = new GroupCoordinator(CONFIG);
        AtomicReference<JoinGroupResult> result = new AtomicReference<>();

        coordinator.joinGroup("g1", "", "consumer", List.of(protocol("range")), 10_000, 30_000, result::set);

        assertNull(result.get(), "a lone joiner must wait out the window in case a second member is about to arrive");
    }

    @Test
    void twoJoinsWithinTheWindowResolveTogetherWithTheSameGeneration() throws InterruptedException {
        GroupCoordinator coordinator = new GroupCoordinator(CONFIG);
        AtomicReference<JoinGroupResult> first = new AtomicReference<>();
        AtomicReference<JoinGroupResult> second = new AtomicReference<>();

        coordinator.joinGroup("g1", "", "consumer", List.of(protocol("range")), 10_000, 30_000, first::set);
        coordinator.joinGroup("g1", "", "consumer", List.of(protocol("range")), 10_000, 30_000, second::set);
        assertNull(first.get(), "still waiting on the window even with a second joiner already in");
        assertNull(second.get());

        Thread.sleep(120); // comfortably past the 50ms window
        coordinator.tick();

        assertEquals(Errors.NONE, first.get().errorCode());
        assertEquals(Errors.NONE, second.get().errorCode());
        assertEquals(1, first.get().generationId());
        assertEquals(first.get().generationId(), second.get().generationId(), "both members of one rebalance must land on the same generation");
        assertEquals(first.get().leaderId(), second.get().leaderId(), "both must agree on who the leader is");

        // Exactly one of the two is told it's the leader (gets the member list); the other gets none.
        boolean firstIsLeader = first.get().memberId().equals(first.get().leaderId());
        JoinGroupResult leaderResult = firstIsLeader ? first.get() : second.get();
        JoinGroupResult followerResult = firstIsLeader ? second.get() : first.get();
        assertEquals(2, leaderResult.members().size(), "the leader alone gets every member's subscription metadata");
        assertTrue(followerResult.members().isEmpty(), "a non-leader gets an empty member list");
    }

    @Test
    void syncGroupBarrierWaitsForEveryMemberThenRelaysEachOneItsOwnSlice() throws InterruptedException {
        GroupCoordinator coordinator = new GroupCoordinator(CONFIG);
        AtomicReference<JoinGroupResult> joinA = new AtomicReference<>();
        AtomicReference<JoinGroupResult> joinB = new AtomicReference<>();
        coordinator.joinGroup("g1", "", "consumer", List.of(protocol("range")), 10_000, 30_000, joinA::set);
        coordinator.joinGroup("g1", "", "consumer", List.of(protocol("range")), 10_000, 30_000, joinB::set);
        Thread.sleep(120);
        coordinator.tick();

        String leaderId = joinA.get().leaderId();
        String memberA = joinA.get().memberId();
        String memberB = joinB.get().memberId();
        int generation = joinA.get().generationId();
        boolean aIsLeader = memberA.equals(leaderId);

        AtomicReference<SyncGroupResult> syncA = new AtomicReference<>();
        AtomicReference<SyncGroupResult> syncB = new AtomicReference<>();

        // The leader submits the assignment it "computed" (a real client's
        // job, per PRD §7.1 — here just fabricated bytes to prove relaying).
        List<GroupAssignment> assignments = List.of(
                new GroupAssignment(memberA, "assignment-for-A".getBytes()),
                new GroupAssignment(memberB, "assignment-for-B".getBytes()));

        if (aIsLeader) {
            coordinator.syncGroup("g1", generation, memberA, assignments, syncA::set);
            assertNull(syncB.get());
            coordinator.syncGroup("g1", generation, memberB, List.of(), syncB::set);
        } else {
            coordinator.syncGroup("g1", generation, memberB, assignments, syncB::set);
            assertNull(syncA.get());
            coordinator.syncGroup("g1", generation, memberA, List.of(), syncA::set);
        }

        assertEquals(Errors.NONE, syncA.get().errorCode());
        assertEquals(Errors.NONE, syncB.get().errorCode());
        assertEquals("assignment-for-A", new String(syncA.get().assignment()));
        assertEquals("assignment-for-B", new String(syncB.get().assignment()));
    }

    @Test
    void heartbeatWithStaleGenerationIsRejectedCurrentGenerationSucceeds() throws InterruptedException {
        GroupCoordinator coordinator = new GroupCoordinator(CONFIG);
        AtomicReference<JoinGroupResult> join = new AtomicReference<>();
        coordinator.joinGroup("g1", "", "consumer", List.of(protocol("range")), 10_000, 30_000, join::set);
        Thread.sleep(120);
        coordinator.tick(); // lone joiner finalizes alone once the window elapses

        String memberId = join.get().memberId();
        int currentGeneration = join.get().generationId();

        assertEquals(Errors.ILLEGAL_GENERATION, coordinator.heartbeat("g1", currentGeneration - 1, memberId),
                "a request bearing the previous generation must be rejected — this is the entire fencing mechanism");

        // Still AWAITING_SYNC (nobody's called SyncGroup yet), so the
        // current-generation heartbeat succeeds but flags the rebalance.
        assertEquals(Errors.REBALANCE_IN_PROGRESS, coordinator.heartbeat("g1", currentGeneration, memberId));

        coordinator.syncGroup("g1", currentGeneration, memberId, List.of(new GroupAssignment(memberId, new byte[0])), r -> {});
        assertEquals(Errors.NONE, coordinator.heartbeat("g1", currentGeneration, memberId), "once STABLE, a current-generation heartbeat is a plain success");
    }

    @Test
    void unknownMemberIdIsRejectedWithoutTouchingGroupState() {
        GroupCoordinator coordinator = new GroupCoordinator(CONFIG);
        AtomicReference<JoinGroupResult> result = new AtomicReference<>();
        coordinator.joinGroup("g1", "not-a-real-member-id", "consumer", List.of(protocol("range")), 10_000, 30_000, result::set);

        assertEquals(Errors.UNKNOWN_MEMBER_ID, result.get().errorCode(),
                "a non-empty member_id the coordinator never handed out must be rejected, not silently accepted as new");
    }

    @Test
    void killingOneOfTwoMembersReassignsTheSurvivorWithoutWaitingTheFullRebalanceCeiling() throws InterruptedException {
        GroupCoordinator coordinator = new GroupCoordinator(CONFIG);
        AtomicReference<JoinGroupResult> joinA = new AtomicReference<>();
        AtomicReference<JoinGroupResult> joinB = new AtomicReference<>();
        coordinator.joinGroup("g1", "", "consumer", List.of(protocol("range")), 10_000, 30_000, joinA::set);
        coordinator.joinGroup("g1", "", "consumer", List.of(protocol("range")), 10_000, 30_000, joinB::set);
        Thread.sleep(120);
        coordinator.tick();
        String memberA = joinA.get().memberId();
        String memberB = joinB.get().memberId();
        int generation = joinA.get().generationId();
        coordinator.syncGroup("g1", generation, memberA, List.of(new GroupAssignment(memberA, new byte[0]), new GroupAssignment(memberB, new byte[0])), r -> {});
        coordinator.syncGroup("g1", generation, memberB, List.of(), r -> {});

        // B "dies": stops heartbeating, and A (still alive) leaves explicitly
        // to simulate its own client noticing and re-joining — the coordinator
        // doesn't need to distinguish an explicit leave from a timeout here.
        assertEquals(Errors.NONE, coordinator.leaveGroup("g1", memberB));

        AtomicReference<JoinGroupResult> rejoinA = new AtomicReference<>();
        coordinator.joinGroup("g1", memberA, "consumer", List.of(protocol("range")), 10_000, 30_000, rejoinA::set);

        // The fast path (PRD §7.7 criterion 4: reassigned promptly, not
        // after the full 60s rebalance ceiling): A is the only survivor
        // expected to rejoin, and it just did, so this resolves immediately
        // without needing tick() or any sleep at all.
        assertEquals(Errors.NONE, rejoinA.get().errorCode());
        assertEquals(generation + 1, rejoinA.get().generationId());
        assertEquals(memberA, rejoinA.get().leaderId(), "the sole remaining member is trivially its own leader");
        assertEquals(1, rejoinA.get().members().size());
    }

    @Test
    void electedProtocolIsOneEveryMemberActuallySupports() throws InterruptedException {
        GroupCoordinator coordinator = new GroupCoordinator(CONFIG);
        AtomicReference<JoinGroupResult> joinA = new AtomicReference<>();
        AtomicReference<JoinGroupResult> joinB = new AtomicReference<>();
        // A prefers "roundrobin" then "range"; B only supports "range" —
        // the only name common to both is "range", so that must win even
        // though it's A's second choice.
        coordinator.joinGroup("g1", "", "consumer", List.of(protocol("roundrobin"), protocol("range")), 10_000, 30_000, joinA::set);
        coordinator.joinGroup("g1", "", "consumer", List.of(protocol("range")), 10_000, 30_000, joinB::set);
        Thread.sleep(120);
        coordinator.tick();

        assertEquals("range", joinA.get().protocolName());
        assertEquals("range", joinB.get().protocolName());
    }

    @Test
    void noCommonProtocolIsReportedAsInconsistentGroupProtocolRatherThanHanging() throws InterruptedException {
        GroupCoordinator coordinator = new GroupCoordinator(CONFIG);
        AtomicReference<JoinGroupResult> joinA = new AtomicReference<>();
        AtomicReference<JoinGroupResult> joinB = new AtomicReference<>();
        coordinator.joinGroup("g1", "", "consumer", List.of(protocol("roundrobin")), 10_000, 30_000, joinA::set);
        coordinator.joinGroup("g1", "", "consumer", List.of(protocol("range")), 10_000, 30_000, joinB::set);
        Thread.sleep(120);
        coordinator.tick();

        assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL, joinA.get().errorCode());
        assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL, joinB.get().errorCode());
    }

    @Test
    void invalidSessionTimeoutIsRejectedImmediately() {
        GroupConfig strictConfig = new GroupConfig(6000, 300_000, 60_000, 50);
        GroupCoordinator coordinator = new GroupCoordinator(strictConfig);
        AtomicReference<JoinGroupResult> result = new AtomicReference<>();

        coordinator.joinGroup("g1", "", "consumer", List.of(protocol("range")), 1_000 /* below the 6000 minimum */, 30_000, result::set);

        assertEquals(Errors.INVALID_SESSION_TIMEOUT, result.get().errorCode(), "an out-of-range session timeout must be rejected immediately, not parked");
    }
}
