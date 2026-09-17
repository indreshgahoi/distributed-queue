package io.github.indreshgahoi.queue.metadata.adapter.in.web.internal;

import io.github.indreshgahoi.queue.metadata.domain.model.ProvisioningClaim;

import java.time.Instant;
import java.util.UUID;

public record ProvisioningClaimResponse(
        String tenantId,
        String queueName,
        UUID queueId,
        UUID generationId,
        int partitionId,
        String workerId,
        long registrationEpoch,
        long membershipVersion,
        long fencingToken,
        Instant leaseExpiresAt
) {
    static ProvisioningClaimResponse from(
            ProvisioningClaim claim
    ) {
        return new ProvisioningClaimResponse(
                claim.queue().tenantId(),
                claim.queue().queueName(),
                claim.identity().queueId(),
                claim.identity().generationId(),
                claim.identity().partitionId(),
                claim.identity().workerId(),
                claim.identity().registrationEpoch(),
                claim.identity().membershipVersion(),
                claim.identity().fencingToken(),
                claim.leaseExpiresAt()
        );
    }
}
