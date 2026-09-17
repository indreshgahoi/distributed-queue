package io.github.indreshgahoi.queue.metadata.adapter.in.web.internal;

import io.github.indreshgahoi.queue.metadata.adapter.in.web.QueueResponse;
import io.github.indreshgahoi.queue.metadata.application.port.in.QueueProvisioningUseCase;
import io.github.indreshgahoi.queue.metadata.domain.model.ClaimProvisioningCommand;
import io.github.indreshgahoi.queue.metadata.domain.model.ProvisioningClaimIdentity;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.UUID;

@RestController
@RequestMapping("/internal/v1/provisioning")
@Tag(
        name = "Internal provisioning",
        description = "Trusted queue-node claim and completion API"
)
class QueueProvisioningController {
    private final QueueProvisioningUseCase provisioning;

    QueueProvisioningController(
            QueueProvisioningUseCase provisioning
    ) {
        this.provisioning = provisioning;
    }

    @PostMapping("/claims")
    @Operation(summary = "Claim the next queue awaiting provisioning")
    ResponseEntity<ProvisioningClaimResponse> claim(
            @Valid @RequestBody ClaimProvisioningRequest request
    ) {
        return provisioning.claim(
                        new ClaimProvisioningCommand(
                                request.workerId(),
                                request.registrationEpoch(),
                                Duration.ofSeconds(
                                        request.leaseSeconds()
                                )
                        )
                )
                .map(ProvisioningClaimResponse::from)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.noContent().build());
    }

    @PostMapping("/claims/{queueId}/complete")
    @Operation(summary = "Complete a currently fenced claim")
    QueueResponse complete(
            @PathVariable UUID queueId,
            @Valid @RequestBody ProvisioningClaimRequest request
    ) {
        return QueueResponse.from(
                provisioning.complete(identity(queueId, request))
        );
    }

    @PostMapping("/claims/{queueId}/fail")
    @Operation(summary = "Fail a currently fenced claim")
    QueueResponse fail(
            @PathVariable UUID queueId,
            @Valid @RequestBody ProvisioningClaimRequest request
    ) {
        return QueueResponse.from(
                provisioning.fail(identity(queueId, request))
        );
    }

    private ProvisioningClaimIdentity identity(
            UUID queueId,
            ProvisioningClaimRequest request
    ) {
        return new ProvisioningClaimIdentity(
                queueId,
                request.generationId(),
                request.partitionId(),
                request.workerId(),
                request.registrationEpoch(),
                request.membershipVersion(),
                request.fencingToken()
        );
    }
}
