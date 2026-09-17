package io.github.indreshgahoi.queue.metadata.adapter.out.postgres;

import io.github.indreshgahoi.queue.metadata.application.port.out.NodeTopologyRepository;
import io.github.indreshgahoi.queue.metadata.domain.exception.MetadataUnavailableException;
import io.github.indreshgahoi.queue.metadata.domain.exception.NodeLeaseLostException;
import io.github.indreshgahoi.queue.metadata.domain.exception.PartitionRuntimeAuthorityLostException;
import io.github.indreshgahoi.queue.metadata.domain.exception.QueueNotFoundException;
import io.github.indreshgahoi.queue.metadata.domain.exception.QueueRouteUnavailableException;
import io.github.indreshgahoi.queue.metadata.domain.model.NodeLeaseIdentity;
import io.github.indreshgahoi.queue.metadata.domain.model.NodeRegistration;
import io.github.indreshgahoi.queue.metadata.domain.model.PartitionPlacement;
import io.github.indreshgahoi.queue.metadata.domain.model.PartitionReplicaGroup;
import io.github.indreshgahoi.queue.metadata.domain.model.PartitionRuntimeIdentity;
import io.github.indreshgahoi.queue.metadata.domain.model.PartitionRuntimeState;
import io.github.indreshgahoi.queue.metadata.domain.model.PartitionRuntimeStatus;
import io.github.indreshgahoi.queue.metadata.domain.model.RegisterNodeCommand;
import io.github.indreshgahoi.queue.metadata.domain.model.QueueRoute;
import io.github.indreshgahoi.queue.metadata.domain.model.ReplicaAssignment;
import io.github.indreshgahoi.queue.metadata.domain.model.ReplicaGroupState;
import io.github.indreshgahoi.queue.metadata.domain.model.ReplicaMember;
import io.github.indreshgahoi.queue.metadata.domain.model.ReplicaMemberRole;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Repository
class PostgresNodeTopologyRepository
        implements NodeTopologyRepository {
    private final DataSource dataSource;
    private final Clock clock;

    PostgresNodeTopologyRepository(
            DataSource dataSource,
            Clock clock
    ) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public NodeRegistration register(RegisterNodeCommand command) {
        Objects.requireNonNull(command, "command");
        Instant registeredAt = clock.instant();
        Instant leaseExpiresAt = registeredAt.plus(
                command.leaseDuration()
        );
        String sql = """
                INSERT INTO queue_nodes (
                    node_id,
                    endpoint,
                    registration_epoch,
                    lease_expires_at,
                    registered_at,
                    updated_at
                ) VALUES (?, ?, 1, ?, ?, ?)
                ON CONFLICT (node_id) DO UPDATE
                SET endpoint = EXCLUDED.endpoint,
                    registration_epoch =
                        queue_nodes.registration_epoch + 1,
                    lease_expires_at = EXCLUDED.lease_expires_at,
                    registered_at = EXCLUDED.registered_at,
                    updated_at = EXCLUDED.updated_at
                RETURNING *
                """;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, command.nodeId());
            statement.setString(2, command.endpoint().toString());
            statement.setTimestamp(3, Timestamp.from(leaseExpiresAt));
            statement.setTimestamp(4, Timestamp.from(registeredAt));
            statement.setTimestamp(5, Timestamp.from(registeredAt));
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return mapNode(resultSet);
            }
        } catch (SQLException e) {
            throw databaseFailure("register queue node", e);
        }
    }

    @Override
    public NodeRegistration heartbeat(
            NodeLeaseIdentity identity,
            Duration leaseDuration
    ) {
        Objects.requireNonNull(identity, "identity");
        Objects.requireNonNull(leaseDuration, "leaseDuration");
        if (leaseDuration.isZero() || leaseDuration.isNegative()) {
            throw new IllegalArgumentException(
                    "leaseDuration must be positive"
            );
        }
        Instant heartbeatAt = clock.instant();
        String sql = """
                UPDATE queue_nodes
                SET lease_expires_at = ?,
                    updated_at = ?
                WHERE node_id = ?
                  AND registration_epoch = ?
                  AND lease_expires_at > ?
                RETURNING *
                """;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setTimestamp(
                    1,
                    Timestamp.from(heartbeatAt.plus(leaseDuration))
            );
            statement.setTimestamp(2, Timestamp.from(heartbeatAt));
            statement.setString(3, identity.nodeId());
            statement.setLong(4, identity.registrationEpoch());
            statement.setTimestamp(5, Timestamp.from(heartbeatAt));
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    throw new NodeLeaseLostException();
                }
                return mapNode(resultSet);
            }
        } catch (SQLException e) {
            throw databaseFailure("heartbeat queue node", e);
        }
    }

    @Override
    public List<NodeRegistration> nodes() {
        String sql = """
                SELECT *
                FROM queue_nodes
                ORDER BY node_id
                """;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet resultSet = statement.executeQuery()) {
            List<NodeRegistration> nodes = new ArrayList<>();
            while (resultSet.next()) {
                nodes.add(mapNode(resultSet));
            }
            return List.copyOf(nodes);
        } catch (SQLException e) {
            throw databaseFailure("list queue nodes", e);
        }
    }

    @Override
    public List<PartitionPlacement> placements() {
        String sql = """
                SELECT *
                FROM queue_partition_placements
                ORDER BY queue_id, generation_id, partition_id
                """;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet resultSet = statement.executeQuery()) {
            List<PartitionPlacement> placements = new ArrayList<>();
            while (resultSet.next()) {
                placements.add(new PartitionPlacement(
                        resultSet.getObject("queue_id", java.util.UUID.class),
                        resultSet.getObject(
                                "generation_id",
                                java.util.UUID.class
                        ),
                        resultSet.getInt("partition_id"),
                        resultSet.getString("node_id"),
                        resultSet.getLong("placement_epoch"),
                        resultSet.getLong("metadata_version")
                ));
            }
            return List.copyOf(placements);
        } catch (SQLException e) {
            throw databaseFailure("list partition placements", e);
        }
    }

    @Override
    public List<PartitionReplicaGroup> replicaGroups() {
        String sql = """
                SELECT g.*, r.node_id, r.member_role, r.member_ordinal
                FROM queue_partition_replica_groups g
                LEFT JOIN queue_partition_replicas r
                  ON r.queue_id = g.queue_id
                 AND r.generation_id = g.generation_id
                 AND r.partition_id = g.partition_id
                ORDER BY g.queue_id, g.generation_id, g.partition_id,
                         r.member_ordinal
                """;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet resultSet = statement.executeQuery()) {
            Map<ReplicaGroupKey, ReplicaGroupAccumulator> groups =
                    new LinkedHashMap<>();
            while (resultSet.next()) {
                ReplicaGroupKey key = new ReplicaGroupKey(
                        resultSet.getObject("queue_id", UUID.class),
                        resultSet.getObject("generation_id", UUID.class),
                        resultSet.getInt("partition_id")
                );
                ReplicaGroupAccumulator group = groups.get(key);
                if (group == null) {
                    group = new ReplicaGroupAccumulator(
                            key,
                            resultSet.getInt("replication_factor"),
                            resultSet.getLong("membership_version"),
                            ReplicaGroupState.valueOf(
                                    resultSet.getString("group_state")
                            ),
                            Optional.ofNullable(resultSet.getString(
                                    "bootstrap_leader_node_id"
                            )),
                            new ArrayList<>()
                    );
                    groups.put(key, group);
                }
                String nodeId = resultSet.getString("node_id");
                if (nodeId != null) {
                    group.members().add(new ReplicaMember(
                            nodeId,
                            ReplicaMemberRole.valueOf(
                                    resultSet.getString("member_role")
                            ),
                            resultSet.getInt("member_ordinal")
                    ));
                }
            }
            return groups.values().stream()
                    .map(ReplicaGroupAccumulator::toDomain)
                    .toList();
        } catch (SQLException e) {
            throw databaseFailure("list replica groups", e);
        }
    }

    @Override
    public List<ReplicaAssignment> replicaAssignments(
            NodeLeaseIdentity identity
    ) {
        Objects.requireNonNull(identity, "identity");
        Instant queriedAt = clock.instant();
        String sql = """
                SELECT r.queue_id, r.generation_id, r.partition_id,
                       r.membership_version, r.member_role,
                       (g.bootstrap_leader_node_id = r.node_id)
                           AS bootstrap_leader
                FROM queue_partition_replicas r
                JOIN queue_partition_replica_groups g
                  ON g.queue_id = r.queue_id
                 AND g.generation_id = r.generation_id
                 AND g.partition_id = r.partition_id
                 AND g.membership_version = r.membership_version
                JOIN queue_nodes n ON n.node_id = r.node_id
                WHERE r.node_id = ?
                  AND n.registration_epoch = ?
                  AND n.lease_expires_at > ?
                  AND g.group_state IN ('PROVISIONING', 'ACTIVE')
                ORDER BY r.queue_id, r.generation_id, r.partition_id
                """;
        try (Connection connection = dataSource.getConnection()) {
            requireLiveNode(connection, identity, queriedAt);
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, identity.nodeId());
                statement.setLong(2, identity.registrationEpoch());
                statement.setTimestamp(3, Timestamp.from(queriedAt));
                try (ResultSet resultSet = statement.executeQuery()) {
                    List<ReplicaAssignment> assignments = new ArrayList<>();
                    while (resultSet.next()) {
                        assignments.add(new ReplicaAssignment(
                                resultSet.getObject("queue_id", UUID.class),
                                resultSet.getObject(
                                        "generation_id",
                                        UUID.class
                                ),
                                resultSet.getInt("partition_id"),
                                resultSet.getLong("membership_version"),
                                ReplicaMemberRole.valueOf(
                                        resultSet.getString("member_role")
                                ),
                                resultSet.getBoolean("bootstrap_leader")
                        ));
                    }
                    return List.copyOf(assignments);
                }
            }
        } catch (SQLException e) {
            throw databaseFailure("list replica assignments", e);
        }
    }

    @Override
    public List<PartitionPlacement> activePlacements(
            NodeLeaseIdentity identity
    ) {
        Objects.requireNonNull(identity, "identity");
        Instant queriedAt = clock.instant();
        String sql = """
                SELECT p.*
                FROM queue_partition_placements p
                JOIN queues q
                  ON q.queue_id = p.queue_id
                 AND q.generation_id = p.generation_id
                JOIN queue_nodes n ON n.node_id = p.node_id
                WHERE p.node_id = ?
                  AND n.registration_epoch = ?
                  AND n.lease_expires_at > ?
                  AND q.lifecycle_state = 'ACTIVE'
                ORDER BY p.queue_id, p.generation_id, p.partition_id
                """;
        String authoritySql = """
                SELECT 1 FROM queue_nodes
                WHERE node_id = ?
                  AND registration_epoch = ?
                  AND lease_expires_at > ?
                """;
        try (Connection connection = dataSource.getConnection()) {
            // An empty placement list is valid, but an empty result caused by
            // stale authority must be distinguishable so the node fails closed.
            try (PreparedStatement authority =
                         connection.prepareStatement(authoritySql)) {
                authority.setString(1, identity.nodeId());
                authority.setLong(2, identity.registrationEpoch());
                authority.setTimestamp(3, Timestamp.from(queriedAt));
                try (ResultSet resultSet = authority.executeQuery()) {
                    if (!resultSet.next()) {
                        throw new NodeLeaseLostException();
                    }
                }
            }
            try (PreparedStatement statement =
                         connection.prepareStatement(sql)) {
                statement.setString(1, identity.nodeId());
                statement.setLong(2, identity.registrationEpoch());
                statement.setTimestamp(3, Timestamp.from(queriedAt));
                try (ResultSet resultSet = statement.executeQuery()) {
                    List<PartitionPlacement> placements = new ArrayList<>();
                    while (resultSet.next()) {
                        placements.add(mapPlacement(resultSet));
                    }
                    return List.copyOf(placements);
                }
            }
        } catch (SQLException e) {
            throw databaseFailure("list active partition placements", e);
        }
    }

    @Override
    public PartitionRuntimeStatus publishRuntimeStatus(
            PartitionRuntimeIdentity identity,
            PartitionRuntimeState state,
            String failureReason
    ) {
        Objects.requireNonNull(identity, "identity");
        Objects.requireNonNull(state, "state");
        Instant publishedAt = clock.instant();
        String sql = """
                INSERT INTO queue_partition_runtime_status (
                    queue_id,
                    generation_id,
                    partition_id,
                    node_id,
                    registration_epoch,
                    placement_epoch,
                    runtime_state,
                    failure_reason,
                    updated_at
                )
                SELECT p.queue_id, p.generation_id, p.partition_id,
                       p.node_id, n.registration_epoch,
                       p.placement_epoch, ?, ?, ?
                FROM queue_partition_placements p
                JOIN queue_nodes n ON n.node_id = p.node_id
                JOIN queues q
                  ON q.queue_id = p.queue_id
                 AND q.generation_id = p.generation_id
                WHERE p.queue_id = ?
                  AND p.generation_id = ?
                  AND p.partition_id = ?
                  AND p.node_id = ?
                  AND p.placement_epoch = ?
                  AND n.registration_epoch = ?
                  AND n.lease_expires_at > ?
                  AND q.lifecycle_state = 'ACTIVE'
                ON CONFLICT (queue_id, generation_id, partition_id)
                DO UPDATE SET
                    node_id = EXCLUDED.node_id,
                    registration_epoch = EXCLUDED.registration_epoch,
                    placement_epoch = EXCLUDED.placement_epoch,
                    runtime_state = EXCLUDED.runtime_state,
                    failure_reason = EXCLUDED.failure_reason,
                    updated_at = EXCLUDED.updated_at
                RETURNING *
                """;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, state.name());
            statement.setString(2, failureReason);
            statement.setTimestamp(3, Timestamp.from(publishedAt));
            statement.setObject(4, identity.queueId());
            statement.setObject(5, identity.generationId());
            statement.setInt(6, identity.partitionId());
            statement.setString(7, identity.nodeId());
            statement.setLong(8, identity.placementEpoch());
            statement.setLong(9, identity.registrationEpoch());
            statement.setTimestamp(10, Timestamp.from(publishedAt));
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    throw new PartitionRuntimeAuthorityLostException();
                }
                return mapRuntimeStatus(resultSet);
            }
        } catch (SQLException e) {
            throw databaseFailure("publish partition runtime status", e);
        }
    }

    @Override
    public List<PartitionRuntimeStatus> runtimeStatuses() {
        String sql = """
                SELECT *
                FROM queue_partition_runtime_status
                ORDER BY queue_id, generation_id, partition_id
                """;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet resultSet = statement.executeQuery()) {
            List<PartitionRuntimeStatus> statuses = new ArrayList<>();
            while (resultSet.next()) {
                statuses.add(mapRuntimeStatus(resultSet));
            }
            return List.copyOf(statuses);
        } catch (SQLException e) {
            throw databaseFailure("list partition runtime statuses", e);
        }
    }

    @Override
    public QueueRoute resolveReadyRoute(UUID queueId) {
        Objects.requireNonNull(queueId, "queueId");
        Instant resolvedAt = clock.instant();
        String sql = """
                SELECT q.lifecycle_state,
                       q.generation_id AS queue_generation_id,
                       p.partition_id,
                       p.node_id,
                       p.placement_epoch,
                       n.endpoint,
                       n.registration_epoch,
                       s.runtime_state
                FROM queues q
                LEFT JOIN queue_partition_placements p
                  ON p.queue_id = q.queue_id
                 AND p.generation_id = q.generation_id
                 AND p.partition_id = 0
                LEFT JOIN queue_nodes n
                  ON n.node_id = p.node_id
                 AND n.lease_expires_at > ?
                LEFT JOIN queue_partition_runtime_status s
                  ON s.queue_id = p.queue_id
                 AND s.generation_id = p.generation_id
                 AND s.partition_id = p.partition_id
                 AND s.node_id = p.node_id
                 AND s.placement_epoch = p.placement_epoch
                 AND s.registration_epoch = n.registration_epoch
                 AND s.runtime_state = 'READY'
                WHERE q.queue_id = ?
                """;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setTimestamp(1, Timestamp.from(resolvedAt));
            statement.setObject(2, queueId);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    throw new QueueNotFoundException(queueId);
                }
                if (!"ACTIVE".equals(
                        resultSet.getString("lifecycle_state")
                ) || resultSet.getString("runtime_state") == null) {
                    throw new QueueRouteUnavailableException(queueId);
                }
                return new QueueRoute(
                        queueId,
                        resultSet.getObject(
                                "queue_generation_id",
                                UUID.class
                        ),
                        resultSet.getInt("partition_id"),
                        resultSet.getString("node_id"),
                        URI.create(resultSet.getString("endpoint")),
                        resultSet.getLong("placement_epoch"),
                        resultSet.getLong("registration_epoch")
                );
            }
        } catch (SQLException e) {
            throw databaseFailure("resolve READY queue route", e);
        }
    }

    private void requireLiveNode(
            Connection connection,
            NodeLeaseIdentity identity,
            Instant queriedAt
    ) throws SQLException {
        String sql = """
                SELECT 1
                FROM queue_nodes
                WHERE node_id = ?
                  AND registration_epoch = ?
                  AND lease_expires_at > ?
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, identity.nodeId());
            statement.setLong(2, identity.registrationEpoch());
            statement.setTimestamp(3, Timestamp.from(queriedAt));
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    throw new NodeLeaseLostException();
                }
            }
        }
    }

    private PartitionRuntimeStatus mapRuntimeStatus(ResultSet resultSet)
            throws SQLException {
        PartitionRuntimeIdentity identity = new PartitionRuntimeIdentity(
                resultSet.getObject("queue_id", java.util.UUID.class),
                resultSet.getObject("generation_id", java.util.UUID.class),
                resultSet.getInt("partition_id"),
                resultSet.getString("node_id"),
                resultSet.getLong("registration_epoch"),
                resultSet.getLong("placement_epoch")
        );
        return new PartitionRuntimeStatus(
                identity,
                PartitionRuntimeState.valueOf(
                        resultSet.getString("runtime_state")
                ),
                resultSet.getString("failure_reason"),
                resultSet.getTimestamp("updated_at").toInstant()
        );
    }

    private PartitionPlacement mapPlacement(ResultSet resultSet)
            throws SQLException {
        return new PartitionPlacement(
                resultSet.getObject("queue_id", java.util.UUID.class),
                resultSet.getObject("generation_id", java.util.UUID.class),
                resultSet.getInt("partition_id"),
                resultSet.getString("node_id"),
                resultSet.getLong("placement_epoch"),
                resultSet.getLong("metadata_version")
        );
    }

    private NodeRegistration mapNode(ResultSet resultSet)
            throws SQLException {
        return new NodeRegistration(
                resultSet.getString("node_id"),
                URI.create(resultSet.getString("endpoint")),
                resultSet.getLong("registration_epoch"),
                resultSet.getTimestamp("lease_expires_at").toInstant(),
                resultSet.getTimestamp("registered_at").toInstant(),
                resultSet.getTimestamp("updated_at").toInstant()
        );
    }

    private MetadataUnavailableException databaseFailure(
            String operation,
            SQLException cause
    ) {
        return new MetadataUnavailableException(
                "Failed to " + operation,
                cause
        );
    }

    private record ReplicaGroupKey(
            UUID queueId,
            UUID generationId,
            int partitionId
    ) {
    }

    private record ReplicaGroupAccumulator(
            ReplicaGroupKey key,
            int replicationFactor,
            long membershipVersion,
            ReplicaGroupState state,
            Optional<String> bootstrapLeaderNodeId,
            List<ReplicaMember> members
    ) {
        PartitionReplicaGroup toDomain() {
            return new PartitionReplicaGroup(
                    key.queueId(),
                    key.generationId(),
                    key.partitionId(),
                    replicationFactor,
                    membershipVersion,
                    state,
                    bootstrapLeaderNodeId,
                    members
            );
        }
    }
}
