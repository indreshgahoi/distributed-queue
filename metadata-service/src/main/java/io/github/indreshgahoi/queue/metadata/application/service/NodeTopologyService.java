package io.github.indreshgahoi.queue.metadata.application.service;

import io.github.indreshgahoi.queue.metadata.application.port.in.NodeTopologyUseCase;
import io.github.indreshgahoi.queue.metadata.application.port.out.NodeTopologyRepository;
import io.github.indreshgahoi.queue.metadata.domain.model.NodeLeaseIdentity;
import io.github.indreshgahoi.queue.metadata.domain.model.NodeRegistration;
import io.github.indreshgahoi.queue.metadata.domain.model.PartitionPlacement;
import io.github.indreshgahoi.queue.metadata.domain.model.PartitionRuntimeIdentity;
import io.github.indreshgahoi.queue.metadata.domain.model.PartitionRuntimeState;
import io.github.indreshgahoi.queue.metadata.domain.model.PartitionRuntimeStatus;
import io.github.indreshgahoi.queue.metadata.domain.model.PartitionReplicaGroup;
import io.github.indreshgahoi.queue.metadata.domain.model.RegisterNodeCommand;
import io.github.indreshgahoi.queue.metadata.domain.model.QueueRoute;
import io.github.indreshgahoi.queue.metadata.domain.model.ReplicaAssignment;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

@Service
@Slf4j
final class NodeTopologyService implements NodeTopologyUseCase {
    private final NodeTopologyRepository repository;

    NodeTopologyService(NodeTopologyRepository repository) {
        this.repository = Objects.requireNonNull(repository, "repository");
    }

    @Override
    public NodeRegistration register(RegisterNodeCommand command) {
        NodeRegistration registration = repository.register(command);
        log.info(
                "event=queue_node_registered nodeId={} endpoint={} "
                        + "registrationEpoch={} leaseExpiresAt={}",
                registration.nodeId(),
                registration.endpoint(),
                registration.registrationEpoch(),
                registration.leaseExpiresAt()
        );
        return registration;
    }

    @Override
    public NodeRegistration heartbeat(
            NodeLeaseIdentity identity,
            Duration leaseDuration
    ) {
        return repository.heartbeat(identity, leaseDuration);
    }

    @Override
    public List<NodeRegistration> nodes() {
        return repository.nodes();
    }

    @Override
    public List<PartitionPlacement> placements() {
        return repository.placements();
    }

    @Override
    public List<PartitionReplicaGroup> replicaGroups() {
        return repository.replicaGroups();
    }

    @Override
    public List<ReplicaAssignment> replicaAssignments(
            NodeLeaseIdentity identity
    ) {
        return repository.replicaAssignments(identity);
    }

    @Override
    public List<PartitionPlacement> activePlacements(
            NodeLeaseIdentity identity
    ) {
        return repository.activePlacements(identity);
    }

    @Override
    public PartitionRuntimeStatus publishRuntimeStatus(
            PartitionRuntimeIdentity identity,
            PartitionRuntimeState state,
            String failureReason
    ) {
        if (state == PartitionRuntimeState.READY
                && failureReason != null) {
            throw new IllegalArgumentException(
                    "READY runtime cannot have a failure reason"
            );
        }
        if (state == PartitionRuntimeState.FAILED
                && (failureReason == null || failureReason.isBlank())) {
            throw new IllegalArgumentException(
                    "FAILED runtime requires a failure reason"
            );
        }
        PartitionRuntimeStatus status = repository.publishRuntimeStatus(
                identity,
                state,
                failureReason
        );
        log.info(
                "event=partition_runtime_status_published queueId={} "
                        + "generationId={} partitionId={} nodeId={} "
                        + "registrationEpoch={} placementEpoch={} state={}",
                identity.queueId(),
                identity.generationId(),
                identity.partitionId(),
                identity.nodeId(),
                identity.registrationEpoch(),
                identity.placementEpoch(),
                state
        );
        return status;
    }

    @Override
    public List<PartitionRuntimeStatus> runtimeStatuses() {
        return repository.runtimeStatuses();
    }

    @Override
    public QueueRoute resolveReadyRoute(UUID queueId) {
        return repository.resolveReadyRoute(
                Objects.requireNonNull(queueId, "queueId")
        );
    }
}
