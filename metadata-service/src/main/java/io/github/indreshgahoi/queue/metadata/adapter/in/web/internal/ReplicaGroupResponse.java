package io.github.indreshgahoi.queue.metadata.adapter.in.web.internal;

import io.github.indreshgahoi.queue.metadata.domain.model.PartitionReplicaGroup;
import io.github.indreshgahoi.queue.metadata.domain.model.ReplicaGroupState;
import io.github.indreshgahoi.queue.metadata.domain.model.ReplicaMember;

import java.util.List;
import java.util.UUID;

public record ReplicaGroupResponse(
        UUID queueId,
        UUID generationId,
        int partitionId,
        int replicationFactor,
        long membershipVersion,
        ReplicaGroupState state,
        String bootstrapLeaderNodeId,
        List<ReplicaMember> members
) {
    static ReplicaGroupResponse from(PartitionReplicaGroup group) {
        return new ReplicaGroupResponse(
                group.queueId(),
                group.generationId(),
                group.partitionId(),
                group.replicationFactor(),
                group.membershipVersion(),
                group.state(),
                group.bootstrapLeaderNodeId().orElse(null),
                group.members()
        );
    }
}
