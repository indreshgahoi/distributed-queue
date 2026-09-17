package io.github.indreshgahoi.queue.metadata.domain.model;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public record PartitionReplicaGroup(
        UUID queueId,
        UUID generationId,
        int partitionId,
        int replicationFactor,
        long membershipVersion,
        ReplicaGroupState state,
        Optional<String> bootstrapLeaderNodeId,
        List<ReplicaMember> members
) {
    public PartitionReplicaGroup {
        Objects.requireNonNull(queueId, "queueId");
        Objects.requireNonNull(generationId, "generationId");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(bootstrapLeaderNodeId, "bootstrapLeaderNodeId");
        List<ReplicaMember> immutableMembers = List.copyOf(members);
        members = immutableMembers;
        if (partitionId != 0 || replicationFactor < 1
                || replicationFactor > 7 || membershipVersion <= 0) {
            throw new IllegalArgumentException("Invalid replica group identity");
        }
        if (new HashSet<>(immutableMembers.stream()
                .map(ReplicaMember::nodeId)
                .toList()).size() != immutableMembers.size()) {
            throw new IllegalArgumentException("Replica node IDs must be distinct");
        }
        boolean pending = state == ReplicaGroupState.PENDING_CAPACITY;
        if (pending && (!immutableMembers.isEmpty()
                || bootstrapLeaderNodeId.isPresent())) {
            throw new IllegalArgumentException("Pending group must have no members");
        }
        if (!pending && immutableMembers.size() != replicationFactor) {
            throw new IllegalArgumentException("Replica group must be complete");
        }
        if (!pending && bootstrapLeaderNodeId
                .filter(leader -> immutableMembers.stream().anyMatch(member ->
                        member.nodeId().equals(leader)
                                && member.role() == ReplicaMemberRole.VOTER))
                .isEmpty()) {
            throw new IllegalArgumentException(
                    "Bootstrap leader must be a voter member"
            );
        }
    }
}
