package io.github.indreshgahoi.queue.metadata;

import io.github.indreshgahoi.queue.metadata.domain.model.PartitionReplicaGroup;
import io.github.indreshgahoi.queue.metadata.domain.model.ReplicaAssignment;
import io.github.indreshgahoi.queue.metadata.domain.model.ReplicaGroupState;
import io.github.indreshgahoi.queue.metadata.domain.model.ReplicaMember;
import io.github.indreshgahoi.queue.metadata.domain.model.ReplicaMemberRole;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PartitionReplicaGroupTest {

    @Test
    void pendingCapacityHasNoPartialMembership() {
        assertDoesNotThrow(() -> new PartitionReplicaGroup(
                UUID.randomUUID(),
                UUID.randomUUID(),
                0,
                3,
                1,
                ReplicaGroupState.PENDING_CAPACITY,
                Optional.empty(),
                List.of()
        ));
    }

    @Test
    void activeMembershipMustContainExactlyReplicationFactorMembers() {
        assertThrows(IllegalArgumentException.class, () -> group(
                List.of(member("node-a", 0), member("node-b", 1)),
                "node-a"
        ));
    }

    @Test
    void oneNodeCannotAppearTwiceInAReplicaGroup() {
        assertThrows(IllegalArgumentException.class, () -> group(
                List.of(
                        member("node-a", 0),
                        member("node-a", 1),
                        member("node-c", 2)
                ),
                "node-a"
        ));
    }

    @Test
    void bootstrapLeaderMustBeAVoterMember() {
        assertThrows(IllegalArgumentException.class, () -> group(
                List.of(
                        member("node-a", 0),
                        member("node-b", 1),
                        member("node-c", 2)
                ),
                "node-d"
        ));
        assertThrows(IllegalArgumentException.class, () ->
                new ReplicaAssignment(
                        UUID.randomUUID(),
                        UUID.randomUUID(),
                        0,
                        1,
                        ReplicaMemberRole.LEARNER,
                        true
                )
        );
    }

    private PartitionReplicaGroup group(
            List<ReplicaMember> members,
            String leader
    ) {
        return new PartitionReplicaGroup(
                UUID.randomUUID(),
                UUID.randomUUID(),
                0,
                3,
                1,
                ReplicaGroupState.ACTIVE,
                Optional.of(leader),
                members
        );
    }

    private ReplicaMember member(String nodeId, int ordinal) {
        return new ReplicaMember(nodeId, ReplicaMemberRole.VOTER, ordinal);
    }
}
