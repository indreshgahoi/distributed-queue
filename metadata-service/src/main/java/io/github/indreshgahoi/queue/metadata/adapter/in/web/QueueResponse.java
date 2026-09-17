package io.github.indreshgahoi.queue.metadata.adapter.in.web;

import io.github.indreshgahoi.queue.metadata.domain.model.QueueDescriptor;
import io.github.indreshgahoi.queue.metadata.domain.model.QueueLifecycleState;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.UUID;

@Schema(description = "Authoritative queue metadata")
public record QueueResponse(
        @Schema(example = "acme")
        String tenantId,
        @Schema(example = "orders")
        String queueName,
        @Schema(example = "f8a82bd2-f94d-4de5-8df7-66161975f35b")
        UUID queueId,
        @Schema(example = "653af9a3-36ba-47f5-bd65-209d6b6c78c2")
        UUID generationId,
        @Schema(example = "1")
        int partitionCount,
        @Schema(example = "3")
        int replicationFactor,
        @Schema(example = "PROVISIONING")
        QueueLifecycleState lifecycleState,
        @Schema(example = "0")
        long metadataVersion,
        @Schema(example = "2026-09-01T12:00:00Z")
        Instant createdAt,
        @Schema(example = "2026-09-01T12:00:00Z")
        Instant updatedAt
) {
    public static QueueResponse from(QueueDescriptor descriptor) {
        return new QueueResponse(
                descriptor.tenantId(),
                descriptor.queueName(),
                descriptor.queueId(),
                descriptor.generationId(),
                descriptor.partitionCount(),
                descriptor.replicationFactor(),
                descriptor.lifecycleState(),
                descriptor.metadataVersion(),
                descriptor.createdAt(),
                descriptor.updatedAt()
        );
    }
}
