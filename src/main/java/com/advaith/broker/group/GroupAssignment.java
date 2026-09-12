package com.advaith.broker.group;

/** One entry of the assignment map the group's LEADER submits in its own SyncGroup request — everyone else submits none of these (PRD §7.4). */
public record GroupAssignment(String memberId, byte[] assignment) {}
