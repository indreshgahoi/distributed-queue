package pgcdc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	etcdadapter "github.com/indreshgahoi/distributed-queue/internal/controlplane/adapter/etcd"
	postgresadapter "github.com/indreshgahoi/distributed-queue/internal/controlplane/adapter/postgres"
	"github.com/indreshgahoi/distributed-queue/internal/controlplane/application"
	"github.com/indreshgahoi/distributed-queue/internal/controlplane/domain"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestLogicalReplicationProjectsCommittedOutboxEvent(t *testing.T) {
	databaseURL := os.Getenv("DQ_TEST_CDC_POSTGRES_URL")
	etcdEndpoint := os.Getenv("DQ_TEST_ETCD_ENDPOINT")
	if databaseURL == "" || etcdEndpoint == "" {
		t.Skip("DQ_TEST_CDC_POSTGRES_URL and DQ_TEST_ETCD_ENDPOINT are required")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open PostgreSQL pool: %v", err)
	}
	t.Cleanup(pool.Close)
	applyIntegrationSchema(t, pool)
	if _, err := pool.Exec(ctx, "TRUNCATE dq_partition_replicas, dq_partitions, dq_outbox, dq_metadata_requests, dq_queues RESTART IDENTITY CASCADE"); err != nil {
		t.Fatalf("reset PostgreSQL: %v", err)
	}

	etcdClient, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdEndpoint}, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("open etcd client: %v", err)
	}
	t.Cleanup(func() { _ = etcdClient.Close() })
	if _, err := etcdClient.Delete(ctx, "/dq/v1/", clientv3.WithPrefix()); err != nil {
		t.Fatalf("reset etcd: %v", err)
	}

	replicationConfig, err := pgconn.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse replication config: %v", err)
	}
	replicationConfig.RuntimeParams["replication"] = "database"
	replicationConnection, err := pgconn.ConnectConfig(ctx, replicationConfig)
	if err != nil {
		t.Fatalf("open replication connection: %v", err)
	}
	slot := fmt.Sprintf("dq_test_%d", time.Now().UnixNano())
	startLSN, err := EnsureSlot(ctx, pool, replicationConnection, slot)
	if err != nil {
		t.Fatalf("create replication slot: %v", err)
	}

	streamContext, cancelStream := context.WithCancel(ctx)
	projector := application.NewProjector(etcdadapter.NewProjection(etcdClient))
	stream := NewStream(replicationConnection, slot, "dq_outbox_publication", startLSN, projector)
	streamResult := make(chan error, 1)
	go func() { streamResult <- stream.Run(streamContext) }()

	catalog := postgresadapter.NewCatalog(pool)
	command := domain.CreateQueue{
		TenantID: "tenant-1", QueueName: "orders", IdempotencyKey: "request-1",
		PartitionCount: 4, ReplicationFactor: 3, RequestHash: "hash-1",
		QueueID: "queue-1", GenerationID: "generation-1", RoutingSeed: "seed-1",
		EventID: "event-1", Now: time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC),
	}
	if _, _, err := catalog.CreateQueue(ctx, command); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	projectionKey := "/dq/v1/projections/current/queues/queue-1"
	waitForProjection(t, etcdClient, projectionKey)
	cancelStream()
	if err := <-streamResult; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("stop replication stream: %v", err)
	}
	if err := replicationConnection.Close(ctx); err != nil {
		t.Fatalf("close replication connection: %v", err)
	}
	if _, err := pool.Exec(ctx, "SELECT pg_drop_replication_slot($1)", slot); err != nil {
		t.Fatalf("drop replication slot: %v", err)
	}
}

func applyIntegrationSchema(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	path := filepath.Join("..", "..", "..", "..", "deploy", "postgres", "001_control_plane.sql")
	migration, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	if _, err := pool.Exec(context.Background(), string(migration)); err != nil {
		t.Fatalf("apply migration: %v", err)
	}
}

func waitForProjection(t *testing.T, client *clientv3.Client, key string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		response, err := client.Get(context.Background(), key)
		if err != nil {
			t.Fatalf("read etcd projection: %v", err)
		}
		if len(response.Kvs) == 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("projection %s did not appear before timeout", key)
}
