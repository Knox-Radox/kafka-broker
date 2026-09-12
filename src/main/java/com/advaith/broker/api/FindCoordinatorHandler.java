package com.advaith.broker.api;

import com.advaith.broker.BrokerConfig;
import com.advaith.broker.group.GroupCoordinator;
import com.advaith.broker.protocol.ApiKey;
import com.advaith.broker.protocol.Errors;
import com.advaith.broker.protocol.ProtocolReader;
import com.advaith.broker.protocol.ProtocolWriter;

/**
 * Tells a client which broker is the "group coordinator" for a group id
 * (PRD §7.4). Design decision: even with one broker, always trivially the
 * answer, this implements the REAL lookup shape (hash the group id, mod by
 * the internal offsets topic's partition count) rather than hardcoding
 * "always me" — specifically because M4 needs this computation to already
 * exist, not be invented from scratch, once there's more than one broker
 * for it to actually route between.
 */
public final class FindCoordinatorHandler implements ApiHandler {

    private final BrokerConfig config;
    private final int offsetsTopicPartitions;

    public FindCoordinatorHandler(BrokerConfig config, int offsetsTopicPartitions) {
        this.config = config;
        this.offsetsTopicPartitions = offsetsTopicPartitions;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.FIND_COORDINATOR;
    }

    @Override
    public byte[] handle(RequestContext context, ProtocolReader request) {
        String groupId = request.readString(); // "key" — v0 only supports key_type=GROUP, so this is always a group id

        // Computed for real, even though every group's answer is the same
        // broker right now (see class javadoc) — never referenced past
        // this point because there's nowhere else for it to matter yet.
        GroupCoordinator.coordinatorPartitionFor(groupId, offsetsTopicPartitions);

        ProtocolWriter response = new ProtocolWriter();
        response.writeInt16(Errors.NONE);
        response.writeInt32(config.brokerId());
        response.writeString(config.advertisedHost());
        response.writeInt32(config.advertisedPort());
        return response.toByteArray();
    }
}
