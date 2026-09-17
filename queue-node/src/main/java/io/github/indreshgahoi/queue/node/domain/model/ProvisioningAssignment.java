package io.github.indreshgahoi.queue.node.domain.model;

import io.github.indreshgahoi.queue.storage.StorageLineage;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record ProvisioningAssignment(
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
    public ProvisioningAssignment {
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(queueName, "queueName");
        Objects.requireNonNull(queueId, "queueId");
        Objects.requireNonNull(generationId, "generationId");
        Objects.requireNonNull(workerId, "workerId");
        Objects.requireNonNull(leaseExpiresAt, "leaseExpiresAt");
        if (partitionId != 0 || registrationEpoch <= 0
                || membershipVersion <= 0 || fencingToken <= 0) {
            throw new IllegalArgumentException(
                    "Invalid provisioning assignment"
            );
        }
    }

    public StorageLineage lineage() {
        return new StorageLineage(
                queueId,
                generationId,
                partitionId
        );
    }
}
