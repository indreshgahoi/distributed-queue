package io.github.indreshgahoi.queue.metadata.adapter.in.web.internal;

import io.github.indreshgahoi.queue.metadata.application.port.in.NodeTopologyUseCase;
import io.github.indreshgahoi.queue.metadata.domain.model.NodeLeaseIdentity;
import io.github.indreshgahoi.queue.metadata.domain.model.PartitionRuntimeIdentity;
import io.github.indreshgahoi.queue.metadata.domain.model.RegisterNodeCommand;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/internal/v1")
@Tag(name = "Internal topology", description = "Trusted queue-node topology API")
class NodeTopologyController {
    private final NodeTopologyUseCase topology;

    NodeTopologyController(NodeTopologyUseCase topology) {
        this.topology = topology;
    }

    @PostMapping("/nodes/registrations")
    @Operation(summary = "Register a queue-node process incarnation")
    NodeRegistrationResponse register(
            @Valid @RequestBody RegisterNodeRequest request
    ) {
        return NodeRegistrationResponse.from(topology.register(
                new RegisterNodeCommand(
                        request.nodeId(),
                        request.endpoint(),
                        Duration.ofSeconds(request.leaseSeconds())
                )
        ));
    }

    @PostMapping("/nodes/{nodeId}/heartbeat")
    @Operation(summary = "Renew a current queue-node registration lease")
    NodeRegistrationResponse heartbeat(
            @PathVariable String nodeId,
            @Valid @RequestBody HeartbeatNodeRequest request
    ) {
        return NodeRegistrationResponse.from(topology.heartbeat(
                new NodeLeaseIdentity(
                        nodeId,
                        request.registrationEpoch()
                ),
                Duration.ofSeconds(request.leaseSeconds())
        ));
    }

    @GetMapping("/nodes")
    @Operation(summary = "List queue-node registrations")
    List<NodeRegistrationResponse> nodes() {
        return topology.nodes().stream()
                .map(NodeRegistrationResponse::from)
                .toList();
    }

    @GetMapping("/placements")
    @Operation(summary = "List authoritative partition placements")
    List<PartitionPlacementResponse> placements() {
        return topology.placements().stream()
                .map(PartitionPlacementResponse::from)
                .toList();
    }

    @GetMapping("/replica-groups")
    @Operation(summary = "List desired partition replica groups")
    List<ReplicaGroupResponse> replicaGroups() {
        return topology.replicaGroups().stream()
                .map(ReplicaGroupResponse::from)
                .toList();
    }

    @GetMapping("/nodes/{nodeId}/replica-assignments")
    @Operation(summary = "List desired replicas for a live node incarnation")
    List<ReplicaAssignmentResponse> replicaAssignments(
            @PathVariable String nodeId,
            @org.springframework.web.bind.annotation.RequestParam
            long registrationEpoch
    ) {
        return topology.replicaAssignments(
                        new NodeLeaseIdentity(nodeId, registrationEpoch)
                ).stream()
                .map(ReplicaAssignmentResponse::from)
                .toList();
    }

    @GetMapping("/nodes/{nodeId}/runtime-placements")
    @Operation(summary = "List ACTIVE placements for a live node incarnation")
    List<PartitionPlacementResponse> activePlacements(
            @PathVariable String nodeId,
            @org.springframework.web.bind.annotation.RequestParam
            long registrationEpoch
    ) {
        return topology.activePlacements(
                        new NodeLeaseIdentity(nodeId, registrationEpoch)
                ).stream()
                .map(PartitionPlacementResponse::from)
                .toList();
    }

    @PostMapping(
            "/partitions/{queueId}/{generationId}/{partitionId}/runtime-status"
    )
    @Operation(summary = "Publish fenced partition runtime readiness")
    PartitionRuntimeStatusResponse publishRuntimeStatus(
            @PathVariable UUID queueId,
            @PathVariable UUID generationId,
            @PathVariable int partitionId,
            @Valid @RequestBody PublishRuntimeStatusRequest request
    ) {
        return PartitionRuntimeStatusResponse.from(
                topology.publishRuntimeStatus(
                        new PartitionRuntimeIdentity(
                                queueId,
                                generationId,
                                partitionId,
                                request.nodeId(),
                                request.registrationEpoch(),
                                request.placementEpoch()
                        ),
                        request.state(),
                        request.failureReason()
                )
        );
    }

    @GetMapping("/runtime/partitions")
    @Operation(summary = "List last published partition runtime states")
    List<PartitionRuntimeStatusResponse> runtimeStatuses() {
        return topology.runtimeStatuses().stream()
                .map(PartitionRuntimeStatusResponse::from)
                .toList();
    }

    @GetMapping("/routes/queues/{queueId}")
    @Operation(summary = "Resolve the authoritative READY route for a queue")
    QueueRouteResponse resolveReadyRoute(@PathVariable UUID queueId) {
        return QueueRouteResponse.from(topology.resolveReadyRoute(queueId));
    }
}
