package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/indreshgahoi/distributed-queue/internal/controlplane/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const liveQueueConstraint = "dq_queues_live_tenant_name_unique"

type Catalog struct{ pool *pgxpool.Pool }

func NewCatalog(pool *pgxpool.Pool) *Catalog { return &Catalog{pool: pool} }

func Open(ctx context.Context, databaseURL string) (*Catalog, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return NewCatalog(pool), nil
}

func (c *Catalog) Close() { c.pool.Close() }

func (c *Catalog) Ready(ctx context.Context) error { return c.pool.Ping(ctx) }

func (c *Catalog) CreateQueue(ctx context.Context, command domain.CreateQueue) (domain.QueueGeneration, domain.OutboxEvent, error) {
	if !command.Valid() {
		return domain.QueueGeneration{}, domain.OutboxEvent{}, domain.ErrInvalidQueue
	}
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return domain.QueueGeneration{}, domain.OutboxEvent{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	inserted, err := reserveRequest(ctx, tx, command)
	if err != nil {
		return domain.QueueGeneration{}, domain.OutboxEvent{}, err
	}
	if !inserted {
		queue, event, err := loadPriorRequest(ctx, tx, command)
		if err != nil {
			return domain.QueueGeneration{}, domain.OutboxEvent{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return domain.QueueGeneration{}, domain.OutboxEvent{}, err
		}
		return queue, event, nil
	}

	queue := domain.QueueGeneration{
		TenantID: command.TenantID, QueueID: command.QueueID, QueueName: command.QueueName,
		GenerationID: command.GenerationID, Lifecycle: domain.QueueProvisioning,
		PartitionCount: command.PartitionCount, ReplicationFactor: command.ReplicationFactor,
		RoutingAlgorithmVersion: 1, RoutingSeed: command.RoutingSeed,
		MetadataVersion: 1, CreatedAt: command.Now, UpdatedAt: command.Now,
	}
	payload, err := json.Marshal(queue)
	if err != nil {
		return domain.QueueGeneration{}, domain.OutboxEvent{}, err
	}
	event := domain.OutboxEvent{
		EventID: command.EventID, AggregateType: "QUEUE_GENERATION", AggregateID: queue.QueueID,
		AggregateVersion: queue.MetadataVersion, EventType: "QUEUE_GENERATION_CREATED",
		SchemaVersion: domain.QueueEventSchemaVersion, OccurredAt: command.Now, Payload: payload,
	}

	_, err = tx.Exec(ctx, `
        INSERT INTO dq_queues (
            queue_id, tenant_id, queue_name, generation_id, lifecycle_state,
            partition_count, replication_factor, routing_algorithm_version,
            routing_seed, metadata_version, created_at, updated_at
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
		queue.QueueID, queue.TenantID, queue.QueueName, queue.GenerationID, queue.Lifecycle,
		queue.PartitionCount, queue.ReplicationFactor, queue.RoutingAlgorithmVersion,
		queue.RoutingSeed, queue.MetadataVersion, queue.CreatedAt, queue.UpdatedAt,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == liveQueueConstraint {
			return domain.QueueGeneration{}, domain.OutboxEvent{}, domain.ErrQueueExists
		}
		return domain.QueueGeneration{}, domain.OutboxEvent{}, err
	}
	_, err = tx.Exec(ctx, `
        INSERT INTO dq_outbox (
            event_id, aggregate_type, aggregate_id, aggregate_version,
            event_type, schema_version, occurred_at, payload
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)`,
		event.EventID, event.AggregateType, event.AggregateID, event.AggregateVersion,
		event.EventType, event.SchemaVersion, event.OccurredAt, event.Payload,
	)
	if err != nil {
		return domain.QueueGeneration{}, domain.OutboxEvent{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.QueueGeneration{}, domain.OutboxEvent{}, err
	}
	return queue, event, nil
}

func reserveRequest(ctx context.Context, tx pgx.Tx, command domain.CreateQueue) (bool, error) {
	result, err := tx.Exec(ctx, `
        INSERT INTO dq_metadata_requests (
            tenant_id, idempotency_key, operation_type, request_hash,
            response_queue_id, response_event_id, created_at
        ) VALUES ($1,$2,'CREATE_QUEUE',$3,$4,$5,$6)
        ON CONFLICT (tenant_id, idempotency_key) DO NOTHING`,
		command.TenantID, command.IdempotencyKey, command.RequestHash,
		command.QueueID, command.EventID, command.Now,
	)
	return result.RowsAffected() == 1, err
}

func loadPriorRequest(ctx context.Context, tx pgx.Tx, command domain.CreateQueue) (domain.QueueGeneration, domain.OutboxEvent, error) {
	var requestHash, queueID, eventID string
	err := tx.QueryRow(ctx, `
        SELECT request_hash, response_queue_id, response_event_id
        FROM dq_metadata_requests
        WHERE tenant_id = $1 AND idempotency_key = $2`,
		command.TenantID, command.IdempotencyKey,
	).Scan(&requestHash, &queueID, &eventID)
	if err != nil {
		return domain.QueueGeneration{}, domain.OutboxEvent{}, err
	}
	if requestHash != command.RequestHash {
		return domain.QueueGeneration{}, domain.OutboxEvent{}, domain.ErrIdempotencyConflict
	}
	queue, err := loadQueue(ctx, tx, queueID)
	if err != nil {
		return domain.QueueGeneration{}, domain.OutboxEvent{}, err
	}
	event, err := loadEvent(ctx, tx, eventID)
	return queue, event, err
}

func loadQueue(ctx context.Context, tx pgx.Tx, queueID string) (domain.QueueGeneration, error) {
	var queue domain.QueueGeneration
	err := tx.QueryRow(ctx, `
        SELECT tenant_id, queue_id, queue_name, generation_id, lifecycle_state,
               partition_count, replication_factor, routing_algorithm_version,
               routing_seed, metadata_version, created_at, updated_at
        FROM dq_queues WHERE queue_id = $1`, queueID,
	).Scan(
		&queue.TenantID, &queue.QueueID, &queue.QueueName, &queue.GenerationID, &queue.Lifecycle,
		&queue.PartitionCount, &queue.ReplicationFactor, &queue.RoutingAlgorithmVersion,
		&queue.RoutingSeed, &queue.MetadataVersion, &queue.CreatedAt, &queue.UpdatedAt,
	)
	return queue, err
}

func loadEvent(ctx context.Context, tx pgx.Tx, eventID string) (domain.OutboxEvent, error) {
	var event domain.OutboxEvent
	err := tx.QueryRow(ctx, `
        SELECT event_id, aggregate_type, aggregate_id, aggregate_version,
               event_type, schema_version, occurred_at, payload
        FROM dq_outbox WHERE event_id = $1`, eventID,
	).Scan(
		&event.EventID, &event.AggregateType, &event.AggregateID, &event.AggregateVersion,
		&event.EventType, &event.SchemaVersion, &event.OccurredAt, &event.Payload,
	)
	if err != nil {
		return domain.OutboxEvent{}, fmt.Errorf("load idempotent outbox event: %w", err)
	}
	return event, nil
}
