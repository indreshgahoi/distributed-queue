package io.github.indreshgahoi.queue.metadata.adapter.in.web.internal;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.util.UUID;

public record ProvisioningClaimRequest(
        @NotNull UUID generationId,
        @Min(0) int partitionId,
        @NotBlank @Size(max = 255) String workerId,
        @Min(1) long registrationEpoch,
        @Min(1) long membershipVersion,
        @Min(1) long fencingToken
) {
}
