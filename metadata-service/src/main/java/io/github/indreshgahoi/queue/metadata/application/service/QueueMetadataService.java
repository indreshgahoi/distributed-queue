package io.github.indreshgahoi.queue.metadata.application.service;

import io.github.indreshgahoi.queue.metadata.application.port.in.QueueCatalogUseCase;
import io.github.indreshgahoi.queue.metadata.application.port.in.QueueLifecycleUseCase;
import io.github.indreshgahoi.queue.metadata.application.port.in.QueueProvisioningUseCase;
import io.github.indreshgahoi.queue.metadata.application.port.out.QueueMetadataRepository;
import io.github.indreshgahoi.queue.metadata.domain.model.CreateQueueCommand;
import io.github.indreshgahoi.queue.metadata.domain.model.ClaimProvisioningCommand;
import io.github.indreshgahoi.queue.metadata.domain.model.ProvisioningClaim;
import io.github.indreshgahoi.queue.metadata.domain.model.ProvisioningClaimIdentity;
import io.github.indreshgahoi.queue.metadata.domain.model.QueueDescriptor;
import io.github.indreshgahoi.queue.metadata.domain.model.QueueLifecycleState;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Service
@Slf4j
final class QueueMetadataService
        implements QueueCatalogUseCase,
        QueueLifecycleUseCase,
        QueueProvisioningUseCase {

    private final QueueMetadataRepository repository;

    QueueMetadataService(
            QueueMetadataRepository repository
    ) {
        this.repository = Objects.requireNonNull(
                repository,
                "repository"
        );
    }

    public QueueDescriptor createQueue(
            CreateQueueCommand command
    ) {
        QueueDescriptor queue = repository.create(command);
        log.info(
                "event=queue_create_resolved tenantId={} queueName={} "
                        + "queueId={} generationId={} state={} "
                        + "metadataVersion={}",
                queue.tenantId(),
                queue.queueName(),
                queue.queueId(),
                queue.generationId(),
                queue.lifecycleState(),
                queue.metadataVersion()
        );
        return queue;
    }

    public Optional<QueueDescriptor> getQueue(
            String tenantId,
            String queueName
    ) {
        return repository.find(tenantId, queueName);
    }

    @Override
    public Optional<ProvisioningClaim> claim(
            ClaimProvisioningCommand command
    ) {
        Optional<ProvisioningClaim> claimed =
                repository.claimProvisioning(command);
        claimed.ifPresent(claim -> log.info(
                "event=provisioning_claim_granted queueId={} "
                        + "generationId={} partitionId={} workerId={} "
                        + "registrationEpoch={} membershipVersion={} "
                        + "fencingToken={} leaseExpiresAt={}",
                claim.identity().queueId(),
                claim.identity().generationId(),
                claim.identity().partitionId(),
                claim.identity().workerId(),
                claim.identity().registrationEpoch(),
                claim.identity().membershipVersion(),
                claim.identity().fencingToken(),
                claim.leaseExpiresAt()
        ));
        return claimed;
    }

    @Override
    public QueueDescriptor complete(
            ProvisioningClaimIdentity claim
    ) {
        QueueDescriptor queue =
                repository.completeProvisioning(claim);
        logProvisioningResult(
                "provisioning_claim_completed",
                claim,
                queue
        );
        return queue;
    }

    @Override
    public QueueDescriptor fail(
            ProvisioningClaimIdentity claim
    ) {
        QueueDescriptor queue = repository.failProvisioning(claim);
        logProvisioningResult(
                "provisioning_claim_failed",
                claim,
                queue
        );
        return queue;
    }

    public List<QueueDescriptor> listQueues(
            String tenantId
    ) {
        return repository.list(tenantId);
    }

    public QueueDescriptor beginDeleteQueue(
            String tenantId,
            String queueName
    ) {
        QueueDescriptor queue = repository.beginDeletion(
                tenantId,
                queueName
        );
        log.info(
                "event=queue_deletion_started tenantId={} queueName={} "
                        + "queueId={} generationId={} metadataVersion={}",
                queue.tenantId(),
                queue.queueName(),
                queue.queueId(),
                queue.generationId(),
                queue.metadataVersion()
        );
        return queue;
    }

    public QueueDescriptor completeDeletion(
            QueueDescriptor expected
    ) {
        return transition(
                expected,
                QueueLifecycleState.DELETING,
                QueueLifecycleState.DELETED
        );
    }

    public QueueDescriptor failDeletion(
            QueueDescriptor expected
    ) {
        return transition(
                expected,
                QueueLifecycleState.DELETING,
                QueueLifecycleState.DELETE_FAILED
        );
    }

    public QueueDescriptor retryDeletion(
            QueueDescriptor expected
    ) {
        return transition(
                expected,
                QueueLifecycleState.DELETE_FAILED,
                QueueLifecycleState.DELETING
        );
    }

    private QueueDescriptor transition(
            QueueDescriptor expected,
            QueueLifecycleState expectedState,
            QueueLifecycleState nextState
    ) {
        Objects.requireNonNull(expected, "expected");
        if (expected.lifecycleState() != expectedState) {
            throw new IllegalArgumentException(
                    "Expected descriptor state " + expectedState
            );
        }
        QueueDescriptor queue = repository.transition(
                expected.queueId(),
                expected.generationId(),
                expected.metadataVersion(),
                expectedState,
                nextState
        );
        log.info(
                "event=queue_lifecycle_transition queueId={} "
                        + "generationId={} previousState={} nextState={} "
                        + "metadataVersion={}",
                queue.queueId(),
                queue.generationId(),
                expectedState,
                nextState,
                queue.metadataVersion()
        );
        return queue;
    }

    private void logProvisioningResult(
            String event,
            ProvisioningClaimIdentity claim,
            QueueDescriptor queue
    ) {
        log.info(
                "event={} queueId={} generationId={} partitionId={} "
                        + "workerId={} registrationEpoch={} "
                        + "membershipVersion={} fencingToken={} state={} "
                        + "metadataVersion={}",
                event,
                claim.queueId(),
                claim.generationId(),
                claim.partitionId(),
                claim.workerId(),
                claim.registrationEpoch(),
                claim.membershipVersion(),
                claim.fencingToken(),
                queue.lifecycleState(),
                queue.metadataVersion()
        );
    }
}
