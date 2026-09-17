CREATE TABLE IF NOT EXISTS dq_queues (
    queue_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    queue_name TEXT NOT NULL,
    generation_id TEXT NOT NULL,
    lifecycle_state TEXT NOT NULL,
    partition_count INTEGER NOT NULL CHECK (partition_count > 0),
    replication_factor INTEGER NOT NULL CHECK (replication_factor > 0),
    routing_algorithm_version INTEGER NOT NULL CHECK (routing_algorithm_version > 0),
    routing_seed TEXT NOT NULL,
    metadata_version BIGINT NOT NULL CHECK (metadata_version > 0),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT dq_queue_lifecycle_valid CHECK (
        lifecycle_state IN ('PROVISIONING', 'ACTIVE', 'DELETING', 'DELETED')
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS dq_queues_live_tenant_name_unique
    ON dq_queues (tenant_id, queue_name)
    WHERE lifecycle_state <> 'DELETED';

CREATE TABLE IF NOT EXISTS dq_metadata_requests (
    tenant_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    operation_type TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    response_queue_id TEXT NOT NULL,
    response_event_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS dq_outbox (
    event_id TEXT PRIMARY KEY,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    aggregate_version BIGINT NOT NULL CHECK (aggregate_version > 0),
    event_type TEXT NOT NULL,
    schema_version INTEGER NOT NULL CHECK (schema_version > 0),
    occurred_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    UNIQUE (aggregate_type, aggregate_id, aggregate_version)
);

CREATE INDEX IF NOT EXISTS dq_outbox_aggregate_index
    ON dq_outbox (aggregate_type, aggregate_id, aggregate_version);

CREATE OR REPLACE FUNCTION dq_reject_outbox_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'dq_outbox is append-only'
        USING ERRCODE = '55000';
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_trigger
        WHERE tgname = 'dq_outbox_append_only'
          AND tgrelid = 'dq_outbox'::regclass
          AND NOT tgisinternal
    ) THEN
        CREATE TRIGGER dq_outbox_append_only
            BEFORE UPDATE OR DELETE ON dq_outbox
            FOR EACH ROW
            EXECUTE FUNCTION dq_reject_outbox_mutation();
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication WHERE pubname = 'dq_outbox_publication'
    ) THEN
        EXECUTE 'CREATE PUBLICATION dq_outbox_publication FOR TABLE dq_outbox WITH (publish = ''insert'')';
    END IF;
END
$$;

ALTER PUBLICATION dq_outbox_publication SET (publish = 'insert');

CREATE SEQUENCE IF NOT EXISTS dq_raft_group_id_seq
    AS BIGINT START WITH 1 INCREMENT BY 1 NO CYCLE;

CREATE TABLE IF NOT EXISTS dq_partitions (
    queue_id TEXT NOT NULL REFERENCES dq_queues (queue_id),
    generation_id TEXT NOT NULL,
    partition_id INTEGER NOT NULL CHECK (partition_id >= 0),
    raft_group_id BIGINT NOT NULL DEFAULT nextval('dq_raft_group_id_seq'),
    membership_version BIGINT NOT NULL DEFAULT 1 CHECK (membership_version > 0),
    lifecycle_state TEXT NOT NULL DEFAULT 'PENDING_CAPACITY',
    PRIMARY KEY (queue_id, generation_id, partition_id),
    UNIQUE (raft_group_id),
    CONSTRAINT dq_partition_lifecycle_valid CHECK (
        lifecycle_state IN ('PENDING_CAPACITY', 'PROVISIONING', 'ACTIVE', 'FAILED')
    )
);

CREATE TABLE IF NOT EXISTS dq_partition_replicas (
    raft_group_id BIGINT NOT NULL REFERENCES dq_partitions (raft_group_id),
    replica_id BIGINT NOT NULL,
    node_id TEXT NOT NULL,
    desired_storage_class TEXT NOT NULL,
    member_role TEXT NOT NULL,
    membership_version BIGINT NOT NULL CHECK (membership_version > 0),
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (raft_group_id, replica_id),
    UNIQUE (raft_group_id, node_id),
    CONSTRAINT dq_replica_role_valid CHECK (member_role IN ('VOTER', 'LEARNER'))
);
