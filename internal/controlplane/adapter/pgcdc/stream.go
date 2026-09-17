package pgcdc

import (
	"context"
	"errors"
	"fmt"

	"github.com/indreshgahoi/distributed-queue/internal/controlplane/application"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

const outputPlugin = "pgoutput"

type Stream struct {
	connection   *pgconn.PgConn
	slot         string
	publication  string
	startLSN     pglogrepl.LSN
	decoder      *Decoder
	processor    *Processor
	acknowledger *replicationAcknowledger
}

func NewStream(
	connection *pgconn.PgConn,
	slot string,
	publication string,
	startLSN pglogrepl.LSN,
	projector *application.Projector,
) *Stream {
	acknowledger := &replicationAcknowledger{connection: connection, lastLSN: startLSN}
	return &Stream{
		connection: connection, slot: slot, publication: publication, startLSN: startLSN,
		decoder: NewDecoder(), processor: NewProcessor(projector, acknowledger),
		acknowledger: acknowledger,
	}
}

func (stream *Stream) Run(ctx context.Context) error {
	if stream.connection == nil || stream.slot == "" || stream.publication == "" {
		return fmt.Errorf("invalid replication stream configuration")
	}
	err := pglogrepl.StartReplication(ctx, stream.connection, stream.slot, stream.startLSN,
		pglogrepl.StartReplicationOptions{
			Mode: pglogrepl.LogicalReplication,
			PluginArgs: []string{
				"proto_version '1'",
				fmt.Sprintf("publication_names '%s'", stream.publication),
			},
		})
	if err != nil {
		return fmt.Errorf("start logical replication: %w", err)
	}

	for {
		message, err := stream.connection.ReceiveMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("receive logical replication message: %w", err)
		}
		copyData, ok := message.(*pgproto3.CopyData)
		if !ok || len(copyData.Data) == 0 {
			continue
		}
		switch copyData.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			keepalive, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("decode primary keepalive: %w", err)
			}
			if keepalive.ReplyRequested {
				if err := stream.acknowledger.reply(ctx); err != nil {
					return err
				}
			}
		case pglogrepl.XLogDataByteID:
			if err := stream.acceptXLogData(ctx, copyData.Data[1:]); err != nil {
				return err
			}
		}
	}
}

func (stream *Stream) acceptXLogData(ctx context.Context, data []byte) error {
	xlog, err := pglogrepl.ParseXLogData(data)
	if err != nil {
		return fmt.Errorf("decode XLogData: %w", err)
	}
	message, err := pglogrepl.Parse(xlog.WALData)
	if err != nil {
		return fmt.Errorf("decode pgoutput message: %w", err)
	}
	transaction, err := stream.decoder.Accept(message)
	if err != nil {
		return err
	}
	if transaction == nil {
		return nil
	}
	return stream.processor.Process(ctx, *transaction)
}

type replicationAcknowledger struct {
	connection *pgconn.PgConn
	lastLSN    pglogrepl.LSN
}

func (acknowledger *replicationAcknowledger) Acknowledge(ctx context.Context, lsn pglogrepl.LSN) error {
	if lsn < acknowledger.lastLSN {
		return errors.New("replication acknowledgement cannot move backwards")
	}
	if err := sendStatus(ctx, acknowledger.connection, lsn, false); err != nil {
		return err
	}
	acknowledger.lastLSN = lsn
	return nil
}

func (acknowledger *replicationAcknowledger) reply(ctx context.Context) error {
	return sendStatus(ctx, acknowledger.connection, acknowledger.lastLSN, false)
}

func sendStatus(ctx context.Context, connection *pgconn.PgConn, lsn pglogrepl.LSN, replyRequested bool) error {
	return pglogrepl.SendStandbyStatusUpdate(ctx, connection, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: lsn,
		WALFlushPosition: lsn,
		WALApplyPosition: lsn,
		ReplyRequested:   replyRequested,
	})
}
