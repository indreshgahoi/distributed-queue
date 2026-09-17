package io.github.indreshgahoi.queue.metadata.adapter.in.web;

import io.github.indreshgahoi.queue.metadata.domain.model.CreateQueueCommand;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Size;

@Schema(description = "A request to create a queue in a tenant namespace")
public record CreateQueueRequest(
        @Schema(
                description = "Tenant-local queue name",
                example = "orders"
        )
        @NotBlank @Size(max = 255) String queueName,
        @Schema(
                description = "Number of initial voting replicas; defaults to 3",
                example = "3",
                defaultValue = "3"
        )
        @Min(1) @Max(7) Integer replicationFactor
) {
    int effectiveReplicationFactor() {
        return replicationFactor == null
                ? CreateQueueCommand.DEFAULT_REPLICATION_FACTOR
                : replicationFactor;
    }
}
