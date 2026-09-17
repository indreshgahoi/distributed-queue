package io.github.indreshgahoi.queue.node.adapter.out.http;

import io.github.indreshgahoi.queue.node.application.port.out.ProvisioningMetadataClient;
import io.github.indreshgahoi.queue.node.config.QueueNodeProperties;
import io.github.indreshgahoi.queue.node.domain.model.ProvisioningAssignment;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Component
final class HttpProvisioningMetadataClient
        implements ProvisioningMetadataClient {
    private final RestClient restClient;

    HttpProvisioningMetadataClient(
            RestClient.Builder builder,
            QueueNodeProperties properties
    ) {
        restClient = builder.baseUrl(
                properties.metadataBaseUrl().toString()
        ).build();
    }

    @Override
    public Optional<ProvisioningAssignment> claim(
            String workerId,
            long registrationEpoch,
            Duration leaseDuration
    ) {
        ResponseEntity<ClaimResponse> response = restClient.post()
                .uri("/internal/v1/provisioning/claims")
                .body(
                        new ClaimRequest(
                                workerId,
                                registrationEpoch,
                                leaseDuration.toSeconds()
                        )
                )
                .retrieve()
                .toEntity(ClaimResponse.class);
        return Optional.ofNullable(response.getBody())
                .map(ClaimResponse::toAssignment);
    }

    @Override
    public void complete(ProvisioningAssignment assignment) {
        finish(assignment, "complete");
    }

    @Override
    public void fail(ProvisioningAssignment assignment) {
        finish(assignment, "fail");
    }

    private void finish(
            ProvisioningAssignment assignment,
            String action
    ) {
        restClient.post()
                .uri(
                        "/internal/v1/provisioning/claims/{queueId}/{action}",
                        assignment.queueId(),
                        action
                )
                .body(
                        new FinishRequest(
                                assignment.generationId(),
                                assignment.partitionId(),
                                assignment.workerId(),
                                assignment.registrationEpoch(),
                                assignment.membershipVersion(),
                                assignment.fencingToken()
                        )
                )
                .retrieve()
                .toBodilessEntity();
    }

    private record ClaimRequest(
            String workerId,
            long registrationEpoch,
            long leaseSeconds
    ) {
    }

    private record FinishRequest(
            UUID generationId,
            int partitionId,
            String workerId,
            long registrationEpoch,
            long membershipVersion,
            long fencingToken
    ) {
    }

    private record ClaimResponse(
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
        ProvisioningAssignment toAssignment() {
            return new ProvisioningAssignment(
                    tenantId,
                    queueName,
                    queueId,
                    generationId,
                    partitionId,
                    workerId,
                    registrationEpoch,
                    membershipVersion,
                    fencingToken,
                    leaseExpiresAt
            );
        }
    }
}
