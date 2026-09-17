package io.github.indreshgahoi.queue.metadata.application.port.in;

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

import java.time.Duration;
import java.util.List;
import java.util.UUID;

public interface NodeTopologyUseCase {
    NodeRegistration register(RegisterNodeCommand command);

    NodeRegistration heartbeat(
            NodeLeaseIdentity identity,
            Duration leaseDuration
    );

    List<NodeRegistration> nodes();

    List<PartitionPlacement> placements();

    List<PartitionReplicaGroup> replicaGroups();

    List<ReplicaAssignment> replicaAssignments(NodeLeaseIdentity identity);

    List<PartitionPlacement> activePlacements(
            NodeLeaseIdentity identity
    );

    PartitionRuntimeStatus publishRuntimeStatus(
            PartitionRuntimeIdentity identity,
            PartitionRuntimeState state,
            String failureReason
    );

    List<PartitionRuntimeStatus> runtimeStatuses();

    QueueRoute resolveReadyRoute(UUID queueId);
}
