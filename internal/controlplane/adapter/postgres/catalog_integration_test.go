package postgres

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/indreshgahoi/distributed-queue/internal/controlplane/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestCreateQueueCommitsQueueRequestAndOutboxAtomically(t *testing.T) {
	catalog, pool := integrationCatalog(t)
	command := testCreateQueue("request-1", "hash-1", "queue-1", "event-1")

	queue, event, err := catalog.CreateQueue(context.Background(), command)
	if err != nil {
		t.Fatalf("create queue: %v", err)
	}
	if queue.QueueID != command.QueueID || event.EventID != command.EventID {
		t.Fatalf("unexpected result: queue=%+v event=%+v", queue, event)
	}

	assertRowCount(t, pool, "dq_queues", 1)
	assertRowCount(t, pool, "dq_metadata_requests", 1)
	assertRowCount(t, pool, "dq_outbox", 1)
}

func TestCreateQueueConcurrentRetryReturnsOneDurableResult(t *testing.T) {
	catalog, pool := integrationCatalog(t)
	command := testCreateQueue("request-1", "hash-1", "queue-1", "event-1")

	const callers = 8
	results := make(chan domain.QueueGeneration, callers)
	errorsSeen := make(chan error, callers)
	var group sync.WaitGroup
	for range callers {
		group.Add(1)
		go func() {
			defer group.Done()
			queue, _, err := catalog.CreateQueue(context.Background(), command)
			results <- queue
			errorsSeen <- err
		}()
	}
	group.Wait()
	close(results)
	close(errorsSeen)

	for err := range errorsSeen {
		if err != nil {
			t.Fatalf("concurrent retry failed: %v", err)
		}
	}
	for queue := range results {
		if queue.QueueID != command.QueueID {
			t.Fatalf("retry returned queue %q, want %q", queue.QueueID, command.QueueID)
		}
	}
	assertRowCount(t, pool, "dq_queues", 1)
	assertRowCount(t, pool, "dq_metadata_requests", 1)
	assertRowCount(t, pool, "dq_outbox", 1)
}

func TestCreateQueueRejectsReusedIdempotencyKeyWithDifferentRequest(t *testing.T) {
	catalog, pool := integrationCatalog(t)
	first := testCreateQueue("request-1", "hash-1", "queue-1", "event-1")
	if _, _, err := catalog.CreateQueue(context.Background(), first); err != nil {
		t.Fatalf("create first queue: %v", err)
	}

	conflict := testCreateQueue("request-1", "hash-2", "queue-2", "event-2")
	if _, _, err := catalog.CreateQueue(context.Background(), conflict); !errors.Is(err, domain.ErrIdempotencyConflict) {
		t.Fatalf("got %v, want ErrIdempotencyConflict", err)
	}
	assertRowCount(t, pool, "dq_queues", 1)
	assertRowCount(t, pool, "dq_metadata_requests", 1)
	assertRowCount(t, pool, "dq_outbox", 1)
}

func TestCreateQueueRollsBackRequestReservationWhenLiveNameExists(t *testing.T) {
	catalog, pool := integrationCatalog(t)
	first := testCreateQueue("request-1", "hash-1", "queue-1", "event-1")
	if _, _, err := catalog.CreateQueue(context.Background(), first); err != nil {
		t.Fatalf("create first queue: %v", err)
	}

	duplicate := testCreateQueue("request-2", "hash-2", "queue-2", "event-2")
	if _, _, err := catalog.CreateQueue(context.Background(), duplicate); !errors.Is(err, domain.ErrQueueExists) {
		t.Fatalf("got %v, want ErrQueueExists", err)
	}
	assertRowCount(t, pool, "dq_queues", 1)
	assertRowCount(t, pool, "dq_metadata_requests", 1)
	assertRowCount(t, pool, "dq_outbox", 1)
}

func TestOutboxRowsAreAppendOnly(t *testing.T) {
	catalog, pool := integrationCatalog(t)
	command := testCreateQueue("request-1", "hash-1", "queue-1", "event-1")
	if _, _, err := catalog.CreateQueue(context.Background(), command); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	if _, err := pool.Exec(context.Background(),
		"UPDATE dq_outbox SET event_type = 'CHANGED' WHERE event_id = $1", command.EventID,
	); err == nil {
		t.Fatal("outbox update unexpectedly succeeded")
	}
	if _, err := pool.Exec(context.Background(),
		"DELETE FROM dq_outbox WHERE event_id = $1", command.EventID,
	); err == nil {
		t.Fatal("outbox delete unexpectedly succeeded")
	}
	assertRowCount(t, pool, "dq_outbox", 1)
}

func integrationCatalog(t *testing.T) (*Catalog, *pgxpool.Pool) {
	t.Helper()
	databaseURL := os.Getenv("DQ_TEST_POSTGRES_URL")
	if databaseURL == "" {
		t.Skip("DQ_TEST_POSTGRES_URL is not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open PostgreSQL pool: %v", err)
	}
	t.Cleanup(pool.Close)

	migrationPath := filepath.Join("..", "..", "..", "..", "deploy", "postgres", "001_control_plane.sql")
	migration, err := os.ReadFile(migrationPath)
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	if _, err := pool.Exec(ctx, string(migration)); err != nil {
		t.Fatalf("apply migration: %v", err)
	}
	if _, err := pool.Exec(ctx, "TRUNCATE dq_partition_replicas, dq_partitions, dq_outbox, dq_metadata_requests, dq_queues RESTART IDENTITY CASCADE"); err != nil {
		t.Fatalf("reset integration database: %v", err)
	}
	return NewCatalog(pool), pool
}

func testCreateQueue(idempotencyKey, requestHash, queueID, eventID string) domain.CreateQueue {
	return domain.CreateQueue{
		TenantID:          "tenant-1",
		QueueName:         "orders",
		IdempotencyKey:    idempotencyKey,
		PartitionCount:    4,
		ReplicationFactor: 3,
		RequestHash:       requestHash,
		QueueID:           queueID,
		GenerationID:      "generation-" + queueID,
		RoutingSeed:       "routing-" + queueID,
		EventID:           eventID,
		Now:               time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC),
	}
}

func assertRowCount(t *testing.T, pool *pgxpool.Pool, table string, want int) {
	t.Helper()
	var got int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&got); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	if got != want {
		t.Fatalf("%s has %d rows, want %d", table, got, want)
	}
}
