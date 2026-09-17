package io.github.indreshgahoi.queue.metadata.domain.model;

import java.util.Objects;
import java.util.UUID;

public record ReplicaAssignment(
        UUID queueId,
        UUID generationId,
        int partitionId,
        long membershipVersion,
        ReplicaMemberRole memberRole,
        boolean bootstrapLeader
) {
    public ReplicaAssignment {
        Objects.requireNonNull(queueId, "queueId");
        Objects.requireNonNull(generationId, "generationId");
        Objects.requireNonNull(memberRole, "memberRole");
        if (partitionId != 0 || membershipVersion <= 0) {
            throw new IllegalArgumentException("Invalid replica assignment");
        }
        if (bootstrapLeader && memberRole != ReplicaMemberRole.VOTER) {
            throw new IllegalArgumentException(
                    "Bootstrap leader must be a voter"
            );
        }
    }
}
