package io.github.indreshgahoi.queue.metadata.adapter.out.postgres;

import io.github.indreshgahoi.queue.metadata.application.port.out.QueueMetadataRepository;
import io.github.indreshgahoi.queue.metadata.domain.exception.IdempotencyConflictException;
import io.github.indreshgahoi.queue.metadata.domain.exception.MetadataUnavailableException;
import io.github.indreshgahoi.queue.metadata.domain.exception.NodeLeaseLostException;
import io.github.indreshgahoi.queue.metadata.domain.exception.ProvisioningClaimLostException;
import io.github.indreshgahoi.queue.metadata.domain.exception.QueueAlreadyExistsException;
import io.github.indreshgahoi.queue.metadata.domain.exception.QueueMetadataException;
import io.github.indreshgahoi.queue.metadata.domain.exception.StaleQueueMetadataException;
import io.github.indreshgahoi.queue.metadata.domain.model.CreateQueueCommand;
import io.github.indreshgahoi.queue.metadata.domain.model.ClaimProvisioningCommand;
import io.github.indreshgahoi.queue.metadata.domain.model.ProvisioningClaim;
import io.github.indreshgahoi.queue.metadata.domain.model.ProvisioningClaimIdentity;
import io.github.indreshgahoi.queue.metadata.domain.model.QueueDescriptor;
import io.github.indreshgahoi.queue.metadata.domain.model.QueueLifecycleState;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Repository
class PostgresQueueMetadataRepository
        implements QueueMetadataRepository {

    private static final String OPERATION_CREATE_QUEUE =
            "CREATE_QUEUE";

    private final DataSource dataSource;
    private final Clock clock;

    PostgresQueueMetadataRepository(
            DataSource dataSource,
            Clock clock
    ) {
        this.dataSource = Objects.requireNonNull(
                dataSource,
                "dataSource"
        );
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public QueueDescriptor create(
            CreateQueueCommand command
    ) {
        Objects.requireNonNull(command, "command");
        String requestHash = requestHash(command);

        return inTransaction(connection -> {
            boolean reserved = reserveRequest(
                    connection,
                    command,
                    requestHash
            );

            if (!reserved) {
                return replayCreate(
                        connection,
                        command,
                        requestHash
                );
            }

            QueueDescriptor descriptor =
                    newDescriptor(command);
            insertQueue(connection, descriptor);
            completeRequest(
                    connection,
                    command,
                    descriptor.queueId()
            );
            return descriptor;
        });
    }

    @Override
    public Optional<ProvisioningClaim> claimProvisioning(
            ClaimProvisioningCommand command
    ) {
        Objects.requireNonNull(command, "command");
        return inTransaction(connection -> {
            Instant claimedAt = now();
            requireLiveNodeRegistration(
                    connection,
                    command,
                    claimedAt
            );
            placeNextPendingReplicaGroup(connection, claimedAt);
            Optional<ProvisioningCandidate> candidate =
                    lockProvisioningCandidate(
                            connection,
                            command.workerId(),
                            command.registrationEpoch(),
                            claimedAt
                    );
            if (candidate.isEmpty()) {
                return Optional.empty();
            }
            ProvisioningCandidate selected = candidate.orElseThrow();
            QueueDescriptor queue = selected.queue();
            Instant leaseExpiresAt = claimedAt.plus(
                    command.leaseDuration()
            );
            long token = upsertClaim(
                    connection,
                    queue,
                    command.workerId(),
                    command.registrationEpoch(),
                    selected.membershipVersion(),
                    leaseExpiresAt,
                    claimedAt
            );
            return Optional.of(
                    new ProvisioningClaim(
                            queue,
                            new ProvisioningClaimIdentity(
                                    queue.queueId(),
                                    queue.generationId(),
                                    0,
                                    command.workerId(),
                                    command.registrationEpoch(),
                                    selected.membershipVersion(),
                                    token
                            ),
                            leaseExpiresAt
                    )
            );
        });
    }

    @Override
    public QueueDescriptor completeProvisioning(
            ProvisioningClaimIdentity claim
    ) {
        return finishProvisioning(
                claim,
                QueueLifecycleState.ACTIVE
        );
    }

    @Override
    public QueueDescriptor failProvisioning(
            ProvisioningClaimIdentity claim
    ) {
        return finishProvisioning(
                claim,
                QueueLifecycleState.PROVISIONING_FAILED
        );
    }

    @Override
    public Optional<QueueDescriptor> find(
            String tenantId,
            String queueName
    ) {
        QueueDescriptor.requireText(tenantId, "tenantId");
        QueueDescriptor.requireText(queueName, "queueName");

        String sql = """
                SELECT *
                FROM queues
                WHERE tenant_id = ?
                  AND queue_name = ?
                  AND lifecycle_state <> 'DELETED'
                """;

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement =
                     connection.prepareStatement(sql)) {
            statement.setString(1, tenantId);
            statement.setString(2, queueName);

            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next()
                        ? Optional.of(map(resultSet))
                        : Optional.empty();
            }
        } catch (SQLException e) {
            throw databaseFailure("find queue", e);
        }
    }

    @Override
    public List<QueueDescriptor> list(
            String tenantId
    ) {
        QueueDescriptor.requireText(tenantId, "tenantId");
        String sql = """
                SELECT *
                FROM queues
                WHERE tenant_id = ?
                  AND lifecycle_state <> 'DELETED'
                ORDER BY queue_name
                """;

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement =
                     connection.prepareStatement(sql)) {
            statement.setString(1, tenantId);

            try (ResultSet resultSet = statement.executeQuery()) {
                List<QueueDescriptor> queues = new ArrayList<>();
                while (resultSet.next()) {
                    queues.add(map(resultSet));
                }
                return List.copyOf(queues);
            }
        } catch (SQLException e) {
            throw databaseFailure("list queues", e);
        }
    }

    @Override
    public QueueDescriptor beginDeletion(
            String tenantId,
            String queueName
    ) {
        QueueDescriptor.requireText(tenantId, "tenantId");
        QueueDescriptor.requireText(queueName, "queueName");

        return inTransaction(connection -> {
            QueueDescriptor current = lockByName(
                    connection,
                    tenantId,
                    queueName
            ).orElseThrow(() -> new QueueMetadataException(
                    "Queue does not exist"
            ));

            if (current.lifecycleState()
                    == QueueLifecycleState.DELETING) {
                return current;
            }
            if (current.lifecycleState()
                    != QueueLifecycleState.ACTIVE) {
                throw new QueueMetadataException(
                        "Queue cannot be deleted from state "
                                + current.lifecycleState()
                );
            }
            return updateTransition(
                    connection,
                    current,
                    QueueLifecycleState.DELETING
            );
        });
    }

    @Override
    public QueueDescriptor transition(
            UUID queueId,
            UUID generationId,
            long expectedVersion,
            QueueLifecycleState expectedState,
            QueueLifecycleState nextState
    ) {
        Objects.requireNonNull(queueId, "queueId");
        Objects.requireNonNull(generationId, "generationId");
        Objects.requireNonNull(expectedState, "expectedState");
        Objects.requireNonNull(nextState, "nextState");
        validateTransition(expectedState, nextState);

        return inTransaction(connection -> {
            String sql = """
                    UPDATE queues
                    SET lifecycle_state = ?,
                        metadata_version = metadata_version + 1,
                        updated_at = ?
                    WHERE queue_id = ?
                      AND generation_id = ?
                      AND metadata_version = ?
                      AND lifecycle_state = ?
                    RETURNING *
                    """;

            try (PreparedStatement statement =
                         connection.prepareStatement(sql)) {
                statement.setString(1, nextState.name());
                statement.setTimestamp(
                        2,
                        Timestamp.from(now())
                );
                statement.setObject(3, queueId);
                statement.setObject(4, generationId);
                statement.setLong(5, expectedVersion);
                statement.setString(6, expectedState.name());

                try (ResultSet resultSet =
                             statement.executeQuery()) {
                    if (!resultSet.next()) {
                        throw new StaleQueueMetadataException();
                    }
                    return map(resultSet);
                }
            }
        });
    }

    private boolean reserveRequest(
            Connection connection,
            CreateQueueCommand command,
            String requestHash
    ) throws SQLException {
        String sql = """
                INSERT INTO metadata_requests (
                    tenant_id,
                    idempotency_key,
                    operation_type,
                    request_hash,
                    created_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
                """;

        try (PreparedStatement statement =
                     connection.prepareStatement(sql)) {
            statement.setString(1, command.tenantId());
            statement.setString(2, command.idempotencyKey());
            statement.setString(3, OPERATION_CREATE_QUEUE);
            statement.setString(4, requestHash);
            statement.setTimestamp(
                    5,
                    Timestamp.from(now())
            );
            return statement.executeUpdate() == 1;
        }
    }

    private void requireLiveNodeRegistration(
            Connection connection,
            ClaimProvisioningCommand command,
            Instant claimedAt
    ) throws SQLException {
        String sql = """
                SELECT 1
                FROM queue_nodes
                WHERE node_id = ?
                  AND registration_epoch = ?
                  AND lease_expires_at > ?
                """;
        try (PreparedStatement statement =
                     connection.prepareStatement(sql)) {
            statement.setString(1, command.workerId());
            statement.setLong(2, command.registrationEpoch());
            statement.setTimestamp(3, Timestamp.from(claimedAt));
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    throw new NodeLeaseLostException();
                }
            }
        }
    }

    private void placeNextPendingReplicaGroup(
            Connection connection,
            Instant assignedAt
    ) throws SQLException {
        Optional<PendingReplicaGroup> pending = lockPendingReplicaGroup(
                connection
        );
        if (pending.isEmpty()) {
            return;
        }
        PendingReplicaGroup group = pending.orElseThrow();
        List<String> nodes = selectReplicaNodes(
                connection,
                assignedAt,
                group.replicationFactor()
        );
        if (nodes.size() != group.replicationFactor()) {
            // Membership is durable authority. Publishing a partial group
            // would make later quorum size and recovery decisions ambiguous.
            return;
        }

        insertReplicaMembers(connection, group, nodes, assignedAt);
        activateReplicaGroupPlacement(
                connection,
                group,
                nodes.getFirst(),
                assignedAt
        );
        insertBootstrapPlacement(
                connection,
                group,
                nodes.getFirst(),
                assignedAt
        );
    }

    private Optional<PendingReplicaGroup> lockPendingReplicaGroup(
            Connection connection
    ) throws SQLException {
        String sql = """
                SELECT g.queue_id, g.generation_id, g.partition_id,
                       g.replication_factor, g.membership_version
                FROM queue_partition_replica_groups g
                JOIN queues q
                  ON q.queue_id = g.queue_id
                 AND q.generation_id = g.generation_id
                WHERE q.lifecycle_state = 'PROVISIONING'
                  AND g.group_state = 'PENDING_CAPACITY'
                ORDER BY g.created_at, g.queue_id
                FOR UPDATE OF g SKIP LOCKED
                LIMIT 1
                """;
        try (PreparedStatement statement =
                     connection.prepareStatement(sql);
             ResultSet resultSet = statement.executeQuery()) {
            if (!resultSet.next()) {
                return Optional.empty();
            }
            return Optional.of(new PendingReplicaGroup(
                    resultSet.getObject("queue_id", UUID.class),
                    resultSet.getObject("generation_id", UUID.class),
                    resultSet.getInt("partition_id"),
                    resultSet.getInt("replication_factor"),
                    resultSet.getLong("membership_version")
            ));
        }
    }

    private List<String> selectReplicaNodes(
            Connection connection,
            Instant assignedAt,
            int replicationFactor
    ) throws SQLException {
        String sql = """
                SELECT n.node_id
                FROM queue_nodes n
                LEFT JOIN queue_partition_replicas r
                  ON r.node_id = n.node_id
                LEFT JOIN queues q
                  ON q.queue_id = r.queue_id
                 AND q.generation_id = r.generation_id
                 AND q.lifecycle_state <> 'DELETED'
                WHERE n.lease_expires_at > ?
                GROUP BY n.node_id
                ORDER BY COUNT(q.queue_id), n.node_id
                LIMIT ?
                """;
        try (PreparedStatement statement =
                     connection.prepareStatement(sql)) {
            statement.setTimestamp(1, Timestamp.from(assignedAt));
            statement.setInt(2, replicationFactor);
            try (ResultSet resultSet = statement.executeQuery()) {
                List<String> nodes = new ArrayList<>();
                while (resultSet.next()) {
                    nodes.add(resultSet.getString("node_id"));
                }
                return List.copyOf(nodes);
            }
        }
    }

    private void insertReplicaMembers(
            Connection connection,
            PendingReplicaGroup group,
            List<String> nodes,
            Instant assignedAt
    ) throws SQLException {
        String sql = """
                INSERT INTO queue_partition_replicas (
                    queue_id, generation_id, partition_id, node_id,
                    membership_version, member_role, member_ordinal, created_at
                ) VALUES (?, ?, ?, ?, ?, 'VOTER', ?, ?)
                """;
        try (PreparedStatement statement =
                     connection.prepareStatement(sql)) {
            for (int ordinal = 0; ordinal < nodes.size(); ordinal++) {
                statement.setObject(1, group.queueId());
                statement.setObject(2, group.generationId());
                statement.setInt(3, group.partitionId());
                statement.setString(4, nodes.get(ordinal));
                statement.setLong(5, group.membershipVersion());
                statement.setInt(6, ordinal);
                statement.setTimestamp(7, Timestamp.from(assignedAt));
                statement.addBatch();
            }
            statement.executeBatch();
        }
    }

    private void activateReplicaGroupPlacement(
            Connection connection,
            PendingReplicaGroup group,
            String bootstrapLeader,
            Instant assignedAt
    ) throws SQLException {
        String sql = """
                UPDATE queue_partition_replica_groups
                SET group_state = 'PROVISIONING',
                    bootstrap_leader_node_id = ?,
                    updated_at = ?
                WHERE queue_id = ?
                  AND generation_id = ?
                  AND partition_id = ?
                  AND group_state = 'PENDING_CAPACITY'
                """;
        try (PreparedStatement statement =
                     connection.prepareStatement(sql)) {
            statement.setString(1, bootstrapLeader);
            statement.setTimestamp(2, Timestamp.from(assignedAt));
            statement.setObject(3, group.queueId());
            statement.setObject(4, group.generationId());
            statement.setInt(5, group.partitionId());
            if (statement.executeUpdate() != 1) {
                throw new QueueMetadataException(
                        "Replica group placement authority was lost"
                );
            }
        }
    }

    private void insertBootstrapPlacement(
            Connection connection,
            PendingReplicaGroup group,
            String bootstrapLeader,
            Instant assignedAt
    ) throws SQLException {
        // The existing data plane still consumes one placement. Until leader
        // election replaces it, this row is a compatibility projection of the
        // immutable group's bootstrap leader, not the replica membership.
        String sql = """
                INSERT INTO queue_partition_placements (
                    queue_id, generation_id, partition_id, node_id,
                    placement_epoch, metadata_version, created_at, updated_at
                ) VALUES (?, ?, ?, ?, 1, 0, ?, ?)
                ON CONFLICT DO NOTHING
                """;
        try (PreparedStatement statement =
                     connection.prepareStatement(sql)) {
            statement.setObject(1, group.queueId());
            statement.setObject(2, group.generationId());
            statement.setInt(3, group.partitionId());
            statement.setString(4, bootstrapLeader);
            statement.setTimestamp(5, Timestamp.from(assignedAt));
            statement.setTimestamp(6, Timestamp.from(assignedAt));
            statement.executeUpdate();
        }
    }

    private Optional<ProvisioningCandidate> lockProvisioningCandidate(
            Connection connection,
            String workerId,
            long registrationEpoch,
            Instant claimedAt
    ) throws SQLException {
        String sql = """
                SELECT q.*, r.membership_version
                FROM queues q
                JOIN queue_partition_replica_groups g
                  ON g.queue_id = q.queue_id
                 AND g.generation_id = q.generation_id
                 AND g.partition_id = 0
                JOIN queue_partition_replicas r
                  ON r.queue_id = g.queue_id
                 AND r.generation_id = g.generation_id
                 AND r.partition_id = g.partition_id
                 AND r.membership_version = g.membership_version
                JOIN queue_nodes n
                  ON n.node_id = r.node_id
                WHERE q.lifecycle_state = 'PROVISIONING'
                  AND g.group_state = 'PROVISIONING'
                  AND r.node_id = ?
                  AND n.registration_epoch = ?
                  AND n.lease_expires_at > ?
                  AND NOT EXISTS (
                      SELECT 1
                      FROM queue_partition_replica_runtime_status s
                      WHERE s.queue_id = r.queue_id
                        AND s.generation_id = r.generation_id
                        AND s.partition_id = r.partition_id
                        AND s.node_id = r.node_id
                        AND s.membership_version = r.membership_version
                        AND s.registration_epoch = n.registration_epoch
                        AND s.runtime_state = 'READY'
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM queue_replica_provisioning_claims c
                      WHERE c.queue_id = r.queue_id
                        AND c.generation_id = r.generation_id
                        AND c.partition_id = r.partition_id
                        AND c.node_id = r.node_id
                        AND c.membership_version = r.membership_version
                        AND c.registration_epoch = n.registration_epoch
                        AND c.lease_expires_at > ?
                  )
                ORDER BY q.created_at, q.queue_id
                FOR UPDATE OF r SKIP LOCKED
                LIMIT 1
                """;
        try (PreparedStatement statement =
                     connection.prepareStatement(sql)) {
            statement.setString(1, workerId);
            statement.setLong(2, registrationEpoch);
            statement.setTimestamp(3, Timestamp.from(claimedAt));
            statement.setTimestamp(4, Timestamp.from(claimedAt));
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next()
                        ? Optional.of(new ProvisioningCandidate(
                                map(resultSet),
                                resultSet.getLong("membership_version")
                        ))
                        : Optional.empty();
            }
        }
    }

    private long upsertClaim(
            Connection connection,
            QueueDescriptor queue,
            String workerId,
            long registrationEpoch,
            long membershipVersion,
            Instant leaseExpiresAt,
            Instant claimedAt
    ) throws SQLException {
        String sql = """
                INSERT INTO queue_replica_provisioning_claims (
                    queue_id,
                    generation_id,
                    partition_id,
                    node_id,
                    membership_version,
                    registration_epoch,
                    fencing_token,
                    lease_expires_at,
                    updated_at
                ) VALUES (?, ?, 0, ?, ?, ?, 1, ?, ?)
                ON CONFLICT (
                    queue_id, generation_id, partition_id, node_id
                ) DO UPDATE
                SET membership_version = EXCLUDED.membership_version,
                    registration_epoch = EXCLUDED.registration_epoch,
                    fencing_token =
                        queue_replica_provisioning_claims.fencing_token + 1,
                    lease_expires_at = EXCLUDED.lease_expires_at,
                    updated_at = EXCLUDED.updated_at
                RETURNING fencing_token
                """;
        try (PreparedStatement statement =
                     connection.prepareStatement(sql)) {
            statement.setObject(1, queue.queueId());
            statement.setObject(2, queue.generationId());
            statement.setString(3, workerId);
            statement.setLong(4, membershipVersion);
            statement.setLong(5, registrationEpoch);
            statement.setTimestamp(6, Timestamp.from(leaseExpiresAt));
            statement.setTimestamp(7, Timestamp.from(claimedAt));
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    throw new QueueMetadataException(
                            "Provisioning claim was not recorded"
                    );
                }
                return resultSet.getLong("fencing_token");
            }
        }
    }

    private QueueDescriptor finishProvisioning(
            ProvisioningClaimIdentity claim,
            QueueLifecycleState completedState
    ) {
        Objects.requireNonNull(claim, "claim");
        return inTransaction(connection -> {
            Instant completedAt = now();
            LockedProvisioningClaim current = lockClaim(
                    connection,
                    claim.queueId(),
                    claim.workerId()
            ).orElseThrow(ProvisioningClaimLostException::new);
            if (!current.matches(claim)) {
                throw new ProvisioningClaimLostException();
            }
            QueueDescriptor queue = current.queue();
            if (queue.lifecycleState() == completedState) {
                return queue;
            }
            if (queue.lifecycleState()
                    != QueueLifecycleState.PROVISIONING
                    || !current.authoritativeAt(completedAt)) {
                throw new ProvisioningClaimLostException();
            }
            updateReplicaRuntimeStatus(
                    connection,
                    current,
                    completedState == QueueLifecycleState.ACTIVE
                            ? "READY"
                            : "FAILED",
                    completedAt
            );
            if (completedState == QueueLifecycleState.PROVISIONING_FAILED) {
                updateReplicaGroupState(
                        connection,
                        current,
                        "PROVISIONING_FAILED",
                        completedAt
                );
                return updateTransition(connection, queue, completedState);
            }
            if (!allReplicasReady(connection, current, completedAt)) {
                // Per-member readiness is retained, but serving begins only
                // after the complete configured replica set is materialized.
                return queue;
            }
            updateReplicaGroupState(
                    connection,
                    current,
                    "ACTIVE",
                    completedAt
            );
            return updateTransition(connection, queue, completedState);
        });
    }

    private Optional<LockedProvisioningClaim> lockClaim(
            Connection connection,
            UUID queueId,
            String workerId
    ) throws SQLException {
        String sql = """
                SELECT q.*,
                       c.generation_id AS claim_generation_id,
                       c.partition_id AS claim_partition_id,
                       c.node_id AS claim_worker_id,
                       c.registration_epoch AS claim_registration_epoch,
                       c.membership_version AS claim_membership_version,
                       c.fencing_token AS claim_fencing_token,
                       c.lease_expires_at AS claim_lease_expires_at,
                       g.membership_version AS current_membership_version,
                       n.registration_epoch AS current_registration_epoch,
                       n.lease_expires_at AS node_lease_expires_at
                FROM queues q
                JOIN queue_replica_provisioning_claims c
                  ON c.queue_id = q.queue_id
                JOIN queue_partition_replicas r
                  ON r.queue_id = c.queue_id
                 AND r.generation_id = c.generation_id
                 AND r.partition_id = c.partition_id
                 AND r.node_id = c.node_id
                JOIN queue_partition_replica_groups g
                  ON g.queue_id = r.queue_id
                 AND g.generation_id = r.generation_id
                 AND g.partition_id = r.partition_id
                 AND g.membership_version = r.membership_version
                JOIN queue_nodes n
                  ON n.node_id = r.node_id
                WHERE q.queue_id = ?
                  AND c.node_id = ?
                FOR UPDATE OF q, c
                """;
        try (PreparedStatement statement =
                     connection.prepareStatement(sql)) {
            statement.setObject(1, queueId);
            statement.setString(2, workerId);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return Optional.empty();
                }
                return Optional.of(
                        new LockedProvisioningClaim(
                                map(resultSet),
                                resultSet.getObject(
                                        "claim_generation_id",
                                        UUID.class
                                ),
                                resultSet.getInt("claim_partition_id"),
                                resultSet.getString("claim_worker_id"),
                                resultSet.getLong(
                                        "claim_registration_epoch"
                                ),
                                resultSet.getLong("claim_membership_version"),
                                resultSet.getLong("claim_fencing_token"),
                                resultSet.getTimestamp(
                                        "claim_lease_expires_at"
                                ).toInstant(),
                                resultSet.getLong(
                                        "current_registration_epoch"
                                ),
                                resultSet.getLong(
                                        "current_membership_version"
                                ),
                                resultSet.getTimestamp(
                                        "node_lease_expires_at"
                                ).toInstant()
                        )
                );
            }
        }
    }

    private void updateReplicaRuntimeStatus(
            Connection connection,
            LockedProvisioningClaim claim,
            String state,
            Instant updatedAt
    ) throws SQLException {
        String sql = """
                INSERT INTO queue_partition_replica_runtime_status (
                    queue_id, generation_id, partition_id, node_id,
                    membership_version, registration_epoch,
                    runtime_state, failure_reason, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (
                    queue_id, generation_id, partition_id, node_id
                ) DO UPDATE
                SET membership_version = EXCLUDED.membership_version,
                    registration_epoch = EXCLUDED.registration_epoch,
                    runtime_state = EXCLUDED.runtime_state,
                    failure_reason = EXCLUDED.failure_reason,
                    updated_at = EXCLUDED.updated_at
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, claim.queue().queueId());
            statement.setObject(2, claim.generationId());
            statement.setInt(3, claim.partitionId());
            statement.setString(4, claim.workerId());
            statement.setLong(5, claim.membershipVersion());
            statement.setLong(6, claim.registrationEpoch());
            statement.setString(7, state);
            statement.setString(
                    8,
                    "FAILED".equals(state)
                            ? "Local replica provisioning failed"
                            : null
            );
            statement.setTimestamp(9, Timestamp.from(updatedAt));
            statement.executeUpdate();
        }
    }

    private boolean allReplicasReady(
            Connection connection,
            LockedProvisioningClaim claim,
            Instant checkedAt
    ) throws SQLException {
        String sql = """
                SELECT g.replication_factor,
                       COUNT(*) FILTER (
                           WHERE s.runtime_state = 'READY'
                             AND s.membership_version = g.membership_version
                             AND s.registration_epoch = n.registration_epoch
                             AND n.lease_expires_at > ?
                       ) AS ready_count
                FROM queue_partition_replica_groups g
                JOIN queue_partition_replicas r
                  ON r.queue_id = g.queue_id
                 AND r.generation_id = g.generation_id
                 AND r.partition_id = g.partition_id
                 AND r.membership_version = g.membership_version
                JOIN queue_nodes n ON n.node_id = r.node_id
                LEFT JOIN queue_partition_replica_runtime_status s
                  ON s.queue_id = r.queue_id
                 AND s.generation_id = r.generation_id
                 AND s.partition_id = r.partition_id
                 AND s.node_id = r.node_id
                WHERE g.queue_id = ?
                  AND g.generation_id = ?
                  AND g.partition_id = ?
                  AND g.membership_version = ?
                GROUP BY g.replication_factor
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setTimestamp(1, Timestamp.from(checkedAt));
            statement.setObject(2, claim.queue().queueId());
            statement.setObject(3, claim.generationId());
            statement.setInt(4, claim.partitionId());
            statement.setLong(5, claim.membershipVersion());
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next()
                        && resultSet.getInt("ready_count")
                        == resultSet.getInt("replication_factor");
            }
        }
    }

    private void updateReplicaGroupState(
            Connection connection,
            LockedProvisioningClaim claim,
            String state,
            Instant updatedAt
    ) throws SQLException {
        String sql = """
                UPDATE queue_partition_replica_groups
                SET group_state = ?, updated_at = ?
                WHERE queue_id = ?
                  AND generation_id = ?
                  AND partition_id = ?
                  AND membership_version = ?
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, state);
            statement.setTimestamp(2, Timestamp.from(updatedAt));
            statement.setObject(3, claim.queue().queueId());
            statement.setObject(4, claim.generationId());
            statement.setInt(5, claim.partitionId());
            statement.setLong(6, claim.membershipVersion());
            if (statement.executeUpdate() != 1) {
                throw new ProvisioningClaimLostException();
            }
        }
    }

    private QueueDescriptor replayCreate(
            Connection connection,
            CreateQueueCommand command,
            String requestHash
    ) throws SQLException {
        String sql = """
                SELECT operation_type, request_hash, response_queue_id
                FROM metadata_requests
                WHERE tenant_id = ?
                  AND idempotency_key = ?
                FOR UPDATE
                """;

        try (PreparedStatement statement =
                     connection.prepareStatement(sql)) {
            statement.setString(1, command.tenantId());
            statement.setString(2, command.idempotencyKey());

            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()
                        || !OPERATION_CREATE_QUEUE.equals(
                        resultSet.getString("operation_type")
                )
                        || !requestHash.equals(
                        resultSet.getString("request_hash")
                )) {
                    throw new IdempotencyConflictException();
                }
                UUID queueId = resultSet.getObject(
                        "response_queue_id",
                        UUID.class
                );
                if (queueId == null) {
                    throw new QueueMetadataException(
                            "Idempotent request has no committed response"
                    );
                }
                return findById(connection, queueId)
                        .orElseThrow(() -> new QueueMetadataException(
                                "Idempotent response queue is missing"
                        ));
            }
        }
    }

    private void insertQueue(
            Connection connection,
            QueueDescriptor descriptor
    ) throws SQLException {
        String sql = """
                INSERT INTO queues (
                    queue_id,
                    tenant_id,
                    queue_name,
                    generation_id,
                    lifecycle_state,
                    partition_count,
                    replication_factor,
                    metadata_version,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;

        try (PreparedStatement statement =
                     connection.prepareStatement(sql)) {
            statement.setObject(1, descriptor.queueId());
            statement.setString(2, descriptor.tenantId());
            statement.setString(3, descriptor.queueName());
            statement.setObject(4, descriptor.generationId());
            statement.setString(
                    5,
                    descriptor.lifecycleState().name()
            );
            statement.setInt(6, descriptor.partitionCount());
            statement.setInt(7, descriptor.replicationFactor());
            statement.setLong(8, descriptor.metadataVersion());
            statement.setTimestamp(
                    9,
                    Timestamp.from(descriptor.createdAt())
            );
            statement.setTimestamp(
                    10,
                    Timestamp.from(descriptor.updatedAt())
            );
            statement.executeUpdate();
            insertPendingReplicaGroup(connection, descriptor);
        } catch (SQLException e) {
            if ("23505".equals(e.getSQLState())) {
                throw new QueueAlreadyExistsException(
                        descriptor.tenantId(),
                        descriptor.queueName()
                );
            }
            throw e;
        }
    }

    private void completeRequest(
            Connection connection,
            CreateQueueCommand command,
            UUID queueId
    ) throws SQLException {
        String sql = """
                UPDATE metadata_requests
                SET response_queue_id = ?
                WHERE tenant_id = ?
                  AND idempotency_key = ?
                """;

        try (PreparedStatement statement =
                     connection.prepareStatement(sql)) {
            statement.setObject(1, queueId);
            statement.setString(2, command.tenantId());
            statement.setString(3, command.idempotencyKey());
            if (statement.executeUpdate() != 1) {
                throw new QueueMetadataException(
                        "Failed to complete idempotent request"
                );
            }
        }
    }

    private void insertPendingReplicaGroup(
            Connection connection,
            QueueDescriptor descriptor
    ) throws SQLException {
        String sql = """
                INSERT INTO queue_partition_replica_groups (
                    queue_id,
                    generation_id,
                    partition_id,
                    replication_factor,
                    membership_version,
                    group_state,
                    created_at,
                    updated_at
                ) VALUES (?, ?, 0, ?, 1, 'PENDING_CAPACITY', ?, ?)
                """;
        try (PreparedStatement statement =
                     connection.prepareStatement(sql)) {
            statement.setObject(1, descriptor.queueId());
            statement.setObject(2, descriptor.generationId());
            statement.setInt(3, descriptor.replicationFactor());
            statement.setTimestamp(4, Timestamp.from(descriptor.createdAt()));
            statement.setTimestamp(5, Timestamp.from(descriptor.updatedAt()));
            statement.executeUpdate();
        }
    }

    private Optional<QueueDescriptor> lockByName(
            Connection connection,
            String tenantId,
            String queueName
    ) throws SQLException {
        String sql = """
                SELECT *
                FROM queues
                WHERE tenant_id = ?
                  AND queue_name = ?
                  AND lifecycle_state <> 'DELETED'
                FOR UPDATE
                """;
        try (PreparedStatement statement =
                     connection.prepareStatement(sql)) {
            statement.setString(1, tenantId);
            statement.setString(2, queueName);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next()
                        ? Optional.of(map(resultSet))
                        : Optional.empty();
            }
        }
    }

    private Optional<QueueDescriptor> findById(
            Connection connection,
            UUID queueId
    ) throws SQLException {
        try (PreparedStatement statement =
                     connection.prepareStatement(
                             "SELECT * FROM queues WHERE queue_id = ?"
                     )) {
            statement.setObject(1, queueId);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next()
                        ? Optional.of(map(resultSet))
                        : Optional.empty();
            }
        }
    }

    private QueueDescriptor updateTransition(
            Connection connection,
            QueueDescriptor current,
            QueueLifecycleState nextState
    ) throws SQLException {
        String sql = """
                UPDATE queues
                SET lifecycle_state = ?,
                    metadata_version = metadata_version + 1,
                    updated_at = ?
                WHERE queue_id = ?
                  AND generation_id = ?
                  AND metadata_version = ?
                RETURNING *
                """;
        try (PreparedStatement statement =
                     connection.prepareStatement(sql)) {
            statement.setString(1, nextState.name());
            statement.setTimestamp(
                    2,
                    Timestamp.from(now())
            );
            statement.setObject(3, current.queueId());
            statement.setObject(4, current.generationId());
            statement.setLong(5, current.metadataVersion());
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    throw new StaleQueueMetadataException();
                }
                return map(resultSet);
            }
        }
    }

    private QueueDescriptor newDescriptor(
            CreateQueueCommand command
    ) {
        Instant now = now();
        return new QueueDescriptor(
                command.tenantId(),
                command.queueName(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                1,
                command.replicationFactor(),
                QueueLifecycleState.PROVISIONING,
                0,
                now,
                now
        );
    }

    private void validateTransition(
            QueueLifecycleState expectedState,
            QueueLifecycleState nextState
    ) {
        boolean allowed = switch (expectedState) {
            case PROVISIONING ->
                    nextState == QueueLifecycleState.ACTIVE
                            || nextState
                            == QueueLifecycleState.PROVISIONING_FAILED;
            case PROVISIONING_FAILED ->
                    nextState == QueueLifecycleState.PROVISIONING;
            case ACTIVE ->
                    nextState == QueueLifecycleState.DELETING;
            case DELETING ->
                    nextState == QueueLifecycleState.DELETED
                            || nextState
                            == QueueLifecycleState.DELETE_FAILED;
            case DELETE_FAILED ->
                    nextState == QueueLifecycleState.DELETING;
            case DELETED -> false;
        };
        if (!allowed) {
            throw new IllegalArgumentException(
                    "Invalid queue lifecycle transition: "
                            + expectedState
                            + " -> "
                            + nextState
            );
        }
    }

    private Instant now() {
        return clock.instant()
                .truncatedTo(ChronoUnit.MICROS);
    }

    private QueueDescriptor map(
            ResultSet resultSet
    ) throws SQLException {
        return new QueueDescriptor(
                resultSet.getString("tenant_id"),
                resultSet.getString("queue_name"),
                resultSet.getObject("queue_id", UUID.class),
                resultSet.getObject("generation_id", UUID.class),
                resultSet.getInt("partition_count"),
                resultSet.getInt("replication_factor"),
                QueueLifecycleState.valueOf(
                        resultSet.getString("lifecycle_state")
                ),
                resultSet.getLong("metadata_version"),
                resultSet.getTimestamp("created_at").toInstant(),
                resultSet.getTimestamp("updated_at").toInstant()
        );
    }

    private String requestHash(
            CreateQueueCommand command
    ) {
        try {
            MessageDigest digest =
                    MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(
                    (OPERATION_CREATE_QUEUE
                            + "\u0000"
                            + command.queueName()
                            + "\u0000"
                            + command.replicationFactor())
                            .getBytes(StandardCharsets.UTF_8)
            );
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(
                    "SHA-256 is required by Java",
                    e
            );
        }
    }

    private <T> T inTransaction(
            SqlTransaction<T> transaction
    ) {
        try (Connection connection =
                     dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                T result = transaction.execute(connection);
                connection.commit();
                return result;
            } catch (RuntimeException | SQLException e) {
                rollback(connection, e);
                if (e instanceof RuntimeException runtime) {
                    throw runtime;
                }
                throw databaseFailure(
                        "execute transaction",
                        (SQLException) e
                );
            }
        } catch (SQLException e) {
            throw databaseFailure("open transaction", e);
        }
    }

    private void rollback(
            Connection connection,
            Exception original
    ) {
        try {
            connection.rollback();
        } catch (SQLException rollbackFailure) {
            original.addSuppressed(rollbackFailure);
        }
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

    @FunctionalInterface
    private interface SqlTransaction<T> {
        T execute(Connection connection)
                throws SQLException;
    }

    private record LockedProvisioningClaim(
            QueueDescriptor queue,
            UUID generationId,
            int partitionId,
            String workerId,
            long registrationEpoch,
            long membershipVersion,
            long fencingToken,
            Instant leaseExpiresAt,
            long currentRegistrationEpoch,
            long currentMembershipVersion,
            Instant nodeLeaseExpiresAt
    ) {
        boolean matches(ProvisioningClaimIdentity identity) {
            return queue.queueId().equals(identity.queueId())
                    && generationId.equals(identity.generationId())
                    && partitionId == identity.partitionId()
                    && workerId.equals(identity.workerId())
                    && registrationEpoch
                    == identity.registrationEpoch()
                    && membershipVersion == identity.membershipVersion()
                    && fencingToken == identity.fencingToken();
        }

        boolean authoritativeAt(Instant instant) {
            return leaseExpiresAt.isAfter(instant)
                    && nodeLeaseExpiresAt.isAfter(instant)
                    && registrationEpoch == currentRegistrationEpoch
                    && membershipVersion == currentMembershipVersion;
        }
    }

    private record ProvisioningCandidate(
            QueueDescriptor queue,
            long membershipVersion
    ) {
    }

    private record PendingReplicaGroup(
            UUID queueId,
            UUID generationId,
            int partitionId,
            int replicationFactor,
            long membershipVersion
    ) {
    }
}
