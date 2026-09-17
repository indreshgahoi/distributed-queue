package io.github.indreshgahoi.queue.metadata.adapter.in.web.internal;

import io.github.indreshgahoi.queue.metadata.domain.model.ReplicaAssignment;
import io.github.indreshgahoi.queue.metadata.domain.model.ReplicaMemberRole;

import java.util.UUID;

public record ReplicaAssignmentResponse(
        UUID queueId,
        UUID generationId,
        int partitionId,
        long membershipVersion,
        ReplicaMemberRole memberRole,
        boolean bootstrapLeader
) {
    static ReplicaAssignmentResponse from(ReplicaAssignment assignment) {
        return new ReplicaAssignmentResponse(
                assignment.queueId(),
                assignment.generationId(),
                assignment.partitionId(),
                assignment.membershipVersion(),
                assignment.memberRole(),
                assignment.bootstrapLeader()
        );
    }
}
