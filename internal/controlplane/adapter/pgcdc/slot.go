package pgcdc

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var replicationIdentifier = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

func ValidateReplicationIdentifier(value string) error {
	if !replicationIdentifier.MatchString(value) {
		return fmt.Errorf("invalid PostgreSQL replication identifier %q", value)
	}
	return nil
}

func EnsureSlot(
	ctx context.Context,
	pool *pgxpool.Pool,
	replicationConnection *pgconn.PgConn,
	slot string,
) (pglogrepl.LSN, error) {
	if err := ValidateReplicationIdentifier(slot); err != nil {
		return 0, err
	}
	created, err := pglogrepl.CreateReplicationSlot(ctx, replicationConnection, slot, outputPlugin,
		pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication})
	if err == nil {
		return pglogrepl.ParseLSN(created.ConsistentPoint)
	}
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "42710" {
		return 0, fmt.Errorf("create replication slot: %w", err)
	}

	var value string
	err = pool.QueryRow(ctx, `
        SELECT COALESCE(confirmed_flush_lsn, restart_lsn)::text
        FROM pg_replication_slots
        WHERE slot_name = $1 AND plugin = $2 AND slot_type = 'logical'`,
		slot, outputPlugin,
	).Scan(&value)
	if err != nil {
		return 0, fmt.Errorf("load replication slot position: %w", err)
	}
	return pglogrepl.ParseLSN(value)
}

func RequirePublication(ctx context.Context, pool *pgxpool.Pool, publication string) error {
	if err := ValidateReplicationIdentifier(publication); err != nil {
		return err
	}
	var exists bool
	if err := pool.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)", publication,
	).Scan(&exists); err != nil {
		return fmt.Errorf("check publication: %w", err)
	}
	if !exists {
		return fmt.Errorf("PostgreSQL publication %q does not exist", publication)
	}
	return nil
}
