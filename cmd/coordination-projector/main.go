package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	etcdadapter "github.com/indreshgahoi/distributed-queue/internal/controlplane/adapter/etcd"
	"github.com/indreshgahoi/distributed-queue/internal/controlplane/adapter/pgcdc"
	"github.com/indreshgahoi/distributed-queue/internal/controlplane/application"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	defaultSlot        = "dq_coordination_projector"
	defaultPublication = "dq_outbox_publication"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("coordination projector stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	databaseURL := os.Getenv("DQ_DATABASE_URL")
	if databaseURL == "" {
		return errors.New("DQ_DATABASE_URL is required")
	}
	etcdEndpoints := splitNonEmpty(os.Getenv("DQ_ETCD_ENDPOINTS"))
	if len(etcdEndpoints) == 0 {
		return errors.New("DQ_ETCD_ENDPOINTS is required")
	}
	slot := valueOrDefault(os.Getenv("DQ_CDC_SLOT"), defaultSlot)
	publication := valueOrDefault(os.Getenv("DQ_CDC_PUBLICATION"), defaultPublication)
	if err := pgcdc.ValidateReplicationIdentifier(slot); err != nil {
		return err
	}
	if err := pgcdc.ValidateReplicationIdentifier(publication); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return err
	}
	if err := pgcdc.RequirePublication(ctx, pool, publication); err != nil {
		return err
	}

	replicationConfig, err := pgconn.ParseConfig(databaseURL)
	if err != nil {
		return err
	}
	replicationConfig.RuntimeParams["replication"] = "database"
	replicationConnection, err := pgconn.ConnectConfig(ctx, replicationConfig)
	if err != nil {
		return err
	}
	defer func() { _ = replicationConnection.Close(context.Background()) }()
	startLSN, err := pgcdc.EnsureSlot(ctx, pool, replicationConnection, slot)
	if err != nil {
		return err
	}

	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return err
	}
	defer func() { _ = etcdClient.Close() }()
	projector := application.NewProjector(etcdadapter.NewProjection(etcdClient))
	stream := pgcdc.NewStream(replicationConnection, slot, publication, startLSN, projector)
	logger.Info("coordination projector started",
		"slot", slot,
		"publication", publication,
		"start_lsn", startLSN.String(),
		"etcd_endpoints", len(etcdEndpoints),
	)
	return stream.Run(ctx)
}

func splitNonEmpty(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func valueOrDefault(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}
