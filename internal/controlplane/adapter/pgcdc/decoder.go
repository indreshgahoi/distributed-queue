package pgcdc

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/indreshgahoi/distributed-queue/internal/controlplane/domain"
	"github.com/jackc/pglogrepl"
)

var (
	ErrProtocol       = errors.New("invalid logical replication sequence")
	ErrOutboxMutation = errors.New("outbox must be append-only")
)

type Transaction struct {
	EndLSN pglogrepl.LSN
	Events []domain.OutboxEvent
}

type Decoder struct {
	relations map[uint32]*pglogrepl.RelationMessage
	inTx      bool
	events    []domain.OutboxEvent
}

func NewDecoder() *Decoder {
	return &Decoder{relations: make(map[uint32]*pglogrepl.RelationMessage)}
}

// Accept buffers row changes until PostgreSQL emits Commit. Callers must not
// expose or acknowledge buffered events before the returned transaction exists.
func (decoder *Decoder) Accept(message pglogrepl.Message) (*Transaction, error) {
	switch typed := message.(type) {
	case *pglogrepl.RelationMessage:
		decoder.relations[typed.RelationID] = typed
	case *pglogrepl.BeginMessage:
		if decoder.inTx {
			return nil, fmt.Errorf("%w: begin while transaction is active", ErrProtocol)
		}
		decoder.inTx = true
		decoder.events = nil
	case *pglogrepl.InsertMessage:
		if !decoder.inTx {
			return nil, fmt.Errorf("%w: insert outside transaction", ErrProtocol)
		}
		event, relevant, err := decoder.decodeInsert(typed)
		if err != nil {
			return nil, err
		}
		if relevant {
			decoder.events = append(decoder.events, event)
		}
	case *pglogrepl.UpdateMessage:
		if decoder.isOutboxRelation(typed.RelationID) {
			return nil, ErrOutboxMutation
		}
	case *pglogrepl.DeleteMessage:
		if decoder.isOutboxRelation(typed.RelationID) {
			return nil, ErrOutboxMutation
		}
	case *pglogrepl.TruncateMessage:
		for _, relationID := range typed.RelationIDs {
			if decoder.isOutboxRelation(relationID) {
				return nil, ErrOutboxMutation
			}
		}
	case *pglogrepl.CommitMessage:
		if !decoder.inTx {
			return nil, fmt.Errorf("%w: commit without begin", ErrProtocol)
		}
		transaction := &Transaction{
			EndLSN: typed.TransactionEndLSN,
			Events: append([]domain.OutboxEvent(nil), decoder.events...),
		}
		decoder.inTx = false
		decoder.events = nil
		return transaction, nil
	}
	return nil, nil
}

func (decoder *Decoder) decodeInsert(insert *pglogrepl.InsertMessage) (domain.OutboxEvent, bool, error) {
	relation := decoder.relations[insert.RelationID]
	if relation == nil {
		return domain.OutboxEvent{}, false, fmt.Errorf("%w: unknown relation %d", ErrProtocol, insert.RelationID)
	}
	if relation.Namespace != "public" || relation.RelationName != "dq_outbox" {
		return domain.OutboxEvent{}, false, nil
	}
	if insert.Tuple == nil || len(insert.Tuple.Columns) != len(relation.Columns) {
		return domain.OutboxEvent{}, false, fmt.Errorf("%w: outbox tuple shape changed", ErrProtocol)
	}
	values := make(map[string]string, len(relation.Columns))
	for index, metadata := range relation.Columns {
		column := insert.Tuple.Columns[index]
		if column.DataType != pglogrepl.TupleDataTypeText {
			return domain.OutboxEvent{}, false, fmt.Errorf("%w: outbox column %s is not text", ErrProtocol, metadata.Name)
		}
		values[metadata.Name] = string(column.Data)
	}
	aggregateVersion, err := strconv.ParseUint(values["aggregate_version"], 10, 64)
	if err != nil {
		return domain.OutboxEvent{}, false, fmt.Errorf("decode aggregate version: %w", err)
	}
	schemaVersion, err := strconv.ParseUint(values["schema_version"], 10, 16)
	if err != nil {
		return domain.OutboxEvent{}, false, fmt.Errorf("decode schema version: %w", err)
	}
	occurredAt, err := parsePostgresTime(values["occurred_at"])
	if err != nil {
		return domain.OutboxEvent{}, false, fmt.Errorf("decode occurred_at: %w", err)
	}
	payload := []byte(values["payload"])
	if !json.Valid(payload) {
		return domain.OutboxEvent{}, false, fmt.Errorf("%w: outbox payload is not JSON", ErrProtocol)
	}
	event := domain.OutboxEvent{
		EventID: values["event_id"], AggregateType: values["aggregate_type"],
		AggregateID: values["aggregate_id"], AggregateVersion: aggregateVersion,
		EventType: values["event_type"], SchemaVersion: uint16(schemaVersion),
		OccurredAt: occurredAt, Payload: append([]byte(nil), payload...),
	}
	if event.EventID == "" || event.AggregateType == "" || event.AggregateID == "" || event.EventType == "" {
		return domain.OutboxEvent{}, false, fmt.Errorf("%w: required outbox value is empty", ErrProtocol)
	}
	return event, true, nil
}

func (decoder *Decoder) isOutboxRelation(relationID uint32) bool {
	relation := decoder.relations[relationID]
	return relation != nil && relation.Namespace == "public" && relation.RelationName == "dq_outbox"
}

func parsePostgresTime(value string) (time.Time, error) {
	formats := []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999-07",
	}
	for _, format := range formats {
		if parsed, err := time.Parse(format, value); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported timestamp %q", value)
}
