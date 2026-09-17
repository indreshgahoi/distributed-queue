ALTER TABLE queues
    ADD COLUMN replication_factor INTEGER NOT NULL DEFAULT 3,
    ADD CONSTRAINT queue_lineage_unique
        UNIQUE (queue_id, generation_id),
    ADD CONSTRAINT queue_replication_factor_valid
        CHECK (replication_factor BETWEEN 1 AND 7);

CREATE TABLE queue_partition_replica_groups (
    queue_id UUID NOT NULL,
    generation_id UUID NOT NULL,
    partition_id INTEGER NOT NULL,
    replication_factor INTEGER NOT NULL,
    membership_version BIGINT NOT NULL,
    group_state VARCHAR(32) NOT NULL,
    bootstrap_leader_node_id VARCHAR(255),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (queue_id, generation_id, partition_id),
    UNIQUE (queue_id, generation_id, partition_id, membership_version),
    FOREIGN KEY (queue_id, generation_id)
        REFERENCES queues (queue_id, generation_id),
    CONSTRAINT replica_group_partition_zero CHECK (partition_id = 0),
    CONSTRAINT replica_group_factor_valid
        CHECK (replication_factor BETWEEN 1 AND 7),
    CONSTRAINT replica_group_membership_version_positive
        CHECK (membership_version > 0),
    CONSTRAINT replica_group_state_valid CHECK (
        group_state IN (
            'PENDING_CAPACITY',
            'PROVISIONING',
            'ACTIVE',
            'PROVISIONING_FAILED'
        )
    ),
    CONSTRAINT replica_group_leader_consistent CHECK (
        (group_state = 'PENDING_CAPACITY'
            AND bootstrap_leader_node_id IS NULL)
        OR (group_state <> 'PENDING_CAPACITY'
            AND bootstrap_leader_node_id IS NOT NULL)
    )
);

CREATE TABLE queue_partition_replicas (
    queue_id UUID NOT NULL,
    generation_id UUID NOT NULL,
    partition_id INTEGER NOT NULL,
    node_id VARCHAR(255) NOT NULL REFERENCES queue_nodes (node_id),
    membership_version BIGINT NOT NULL,
    member_role VARCHAR(16) NOT NULL,
    member_ordinal INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (queue_id, generation_id, partition_id, node_id),
    UNIQUE (
        queue_id,
        generation_id,
        partition_id,
        node_id,
        membership_version
    ),
    UNIQUE (queue_id, generation_id, partition_id, member_ordinal),
    FOREIGN KEY (
        queue_id,
        generation_id,
        partition_id,
        membership_version
    )
        REFERENCES queue_partition_replica_groups (
            queue_id,
            generation_id,
            partition_id,
            membership_version
        ),
    CONSTRAINT replica_membership_version_positive
        CHECK (membership_version > 0),
    CONSTRAINT replica_member_role_valid
        CHECK (member_role IN ('VOTER', 'LEARNER')),
    CONSTRAINT replica_member_ordinal_non_negative
        CHECK (member_ordinal >= 0)
);

ALTER TABLE queue_partition_replica_groups
    ADD CONSTRAINT replica_group_bootstrap_member_fk
    FOREIGN KEY (
        queue_id,
        generation_id,
        partition_id,
        bootstrap_leader_node_id
    ) REFERENCES queue_partition_replicas (
        queue_id,
        generation_id,
        partition_id,
        node_id
    ) DEFERRABLE INITIALLY DEFERRED;

CREATE INDEX queue_partition_replica_node_index
    ON queue_partition_replicas (node_id, queue_id, generation_id);

CREATE TABLE queue_partition_replica_runtime_status (
    queue_id UUID NOT NULL,
    generation_id UUID NOT NULL,
    partition_id INTEGER NOT NULL,
    node_id VARCHAR(255) NOT NULL,
    membership_version BIGINT NOT NULL,
    registration_epoch BIGINT NOT NULL,
    runtime_state VARCHAR(16) NOT NULL,
    failure_reason VARCHAR(2048),
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (queue_id, generation_id, partition_id, node_id),
    FOREIGN KEY (
        queue_id,
        generation_id,
        partition_id,
        node_id,
        membership_version
    )
        REFERENCES queue_partition_replicas (
            queue_id,
            generation_id,
            partition_id,
            node_id,
            membership_version
        ),
    CONSTRAINT replica_runtime_membership_version_positive
        CHECK (membership_version > 0),
    CONSTRAINT replica_runtime_registration_epoch_positive
        CHECK (registration_epoch > 0),
    CONSTRAINT replica_runtime_state_valid
        CHECK (runtime_state IN ('READY', 'FAILED')),
    CONSTRAINT replica_runtime_failure_reason_consistent CHECK (
        (runtime_state = 'READY' AND failure_reason IS NULL)
        OR (runtime_state = 'FAILED' AND failure_reason IS NOT NULL)
    )
);

CREATE INDEX queue_partition_replica_runtime_node_index
    ON queue_partition_replica_runtime_status (node_id);

CREATE TABLE queue_replica_provisioning_claims (
    queue_id UUID NOT NULL,
    generation_id UUID NOT NULL,
    partition_id INTEGER NOT NULL,
    node_id VARCHAR(255) NOT NULL,
    membership_version BIGINT NOT NULL,
    registration_epoch BIGINT NOT NULL,
    fencing_token BIGINT NOT NULL,
    lease_expires_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (queue_id, generation_id, partition_id, node_id),
    FOREIGN KEY (
        queue_id,
        generation_id,
        partition_id,
        node_id,
        membership_version
    )
        REFERENCES queue_partition_replicas (
            queue_id,
            generation_id,
            partition_id,
            node_id,
            membership_version
        ),
    CONSTRAINT replica_claim_membership_version_positive
        CHECK (membership_version > 0),
    CONSTRAINT replica_claim_registration_epoch_positive
        CHECK (registration_epoch > 0),
    CONSTRAINT replica_claim_fencing_token_positive
        CHECK (fencing_token > 0)
);

CREATE INDEX queue_replica_claim_expiry_index
    ON queue_replica_provisioning_claims (lease_expires_at);

INSERT INTO queue_partition_replica_groups (
    queue_id,
    generation_id,
    partition_id,
    replication_factor,
    membership_version,
    group_state,
    created_at,
    updated_at
)
SELECT queue_id,
       generation_id,
       0,
       replication_factor,
       1,
       'PENDING_CAPACITY',
       created_at,
       updated_at
FROM queues
WHERE lifecycle_state = 'PROVISIONING';
