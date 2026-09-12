package com.advaith.broker.group;

/**
 * One member's id plus its subscription metadata for the protocol the
 * group elected — exactly what a JoinGroupResponse hands its LEADER for
 * every member, so the leader can compute an assignment (PRD §7.1: the
 * broker never does this itself). Every other member gets an empty list
 * instead of this.
 */
public record MemberSubscription(String memberId, byte[] metadata) {}
