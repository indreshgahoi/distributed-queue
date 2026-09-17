package io.github.indreshgahoi.queue.metadata.domain.model;

import io.github.indreshgahoi.queue.storage.StorageLineage;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record QueueDescriptor(
        String tenantId,
        String queueName,
        UUID queueId,
        UUID generationId,
        int partitionCount,
        int replicationFactor,
        QueueLifecycleState lifecycleState,
        long metadataVersion,
        Instant createdAt,
        Instant updatedAt
) {
    public QueueDescriptor(
            String tenantId,
            String queueName,
            UUID queueId,
            UUID generationId,
            int partitionCount,
            QueueLifecycleState lifecycleState,
            long metadataVersion,
            Instant createdAt,
            Instant updatedAt
    ) {
        this(
                tenantId,
                queueName,
                queueId,
                generationId,
                partitionCount,
                CreateQueueCommand.DEFAULT_REPLICATION_FACTOR,
                lifecycleState,
                metadataVersion,
                createdAt,
                updatedAt
        );
    }

    public QueueDescriptor {
        requireText(tenantId, "tenantId");
        requireText(queueName, "queueName");
        Objects.requireNonNull(queueId, "queueId");
        Objects.requireNonNull(generationId, "generationId");
        Objects.requireNonNull(lifecycleState, "lifecycleState");
        Objects.requireNonNull(createdAt, "createdAt");
        Objects.requireNonNull(updatedAt, "updatedAt");

        if (partitionCount != 1) {
            throw new IllegalArgumentException(
                    "v0.18 supports exactly one partition"
            );
        }
        if (replicationFactor < 1
                || replicationFactor
                > CreateQueueCommand.MAX_REPLICATION_FACTOR) {
            throw new IllegalArgumentException(
                    "Invalid replicationFactor"
            );
        }
        if (metadataVersion < 0) {
            throw new IllegalArgumentException(
                    "metadataVersion must not be negative"
            );
        }
    }

    public StorageLineage storageLineage(
            int partitionId
    ) {
        if (partitionId != 0) {
            throw new IllegalArgumentException(
                    "v0.18 supports only partition 0"
            );
        }
        return new StorageLineage(
                queueId,
                generationId,
                partitionId
        );
    }

    public static void requireText(
            String value,
            String name
    ) {
        Objects.requireNonNull(value, name);
        if (value.isBlank() || value.length() > 255) {
            throw new IllegalArgumentException(
                    name + " must contain 1 to 255 characters"
            );
        }
    }
}
