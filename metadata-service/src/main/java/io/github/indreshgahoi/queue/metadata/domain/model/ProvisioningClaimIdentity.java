package io.github.indreshgahoi.queue.metadata.domain.model;

import java.util.Objects;
import java.util.UUID;

public record ProvisioningClaimIdentity(
        UUID queueId,
        UUID generationId,
        int partitionId,
        String workerId,
        long registrationEpoch,
        long membershipVersion,
        long fencingToken
) {
    public ProvisioningClaimIdentity {
        Objects.requireNonNull(queueId, "queueId");
        Objects.requireNonNull(generationId, "generationId");
        QueueDescriptor.requireText(workerId, "workerId");
        if (partitionId != 0) {
            throw new IllegalArgumentException(
                    "v0.19 supports only partition 0"
            );
        }
        if (registrationEpoch <= 0 || membershipVersion <= 0
                || fencingToken <= 0) {
            throw new IllegalArgumentException(
                    "Claim epochs and fencingToken must be positive"
            );
        }
    }
}
