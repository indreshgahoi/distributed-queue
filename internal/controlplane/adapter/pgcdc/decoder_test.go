package pgcdc

import (
	"errors"
	"testing"

	"github.com/jackc/pglogrepl"
)

func TestDecoderReleasesOutboxEventsOnlyAtCommit(t *testing.T) {
	decoder := NewDecoder()
	relation := outboxRelation(42)
	if transaction, err := decoder.Accept(relation); err != nil || transaction != nil {
		t.Fatalf("relation: transaction=%v err=%v", transaction, err)
	}
	if transaction, err := decoder.Accept(&pglogrepl.BeginMessage{}); err != nil || transaction != nil {
		t.Fatalf("begin: transaction=%v err=%v", transaction, err)
	}
	insert := &pglogrepl.InsertMessage{RelationID: 42, Tuple: outboxTuple()}
	if transaction, err := decoder.Accept(insert); err != nil || transaction != nil {
		t.Fatalf("insert: transaction=%v err=%v", transaction, err)
	}

	transaction, err := decoder.Accept(&pglogrepl.CommitMessage{TransactionEndLSN: 99})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if transaction == nil || transaction.EndLSN != 99 || len(transaction.Events) != 1 {
		t.Fatalf("unexpected transaction: %+v", transaction)
	}
	event := transaction.Events[0]
	if event.EventID != "event-1" || event.AggregateVersion != 1 || event.SchemaVersion != 1 {
		t.Fatalf("unexpected event: %+v", event)
	}
}

func TestDecoderRejectsOutboxUpdate(t *testing.T) {
	decoder := NewDecoder()
	_, _ = decoder.Accept(outboxRelation(42))
	_, _ = decoder.Accept(&pglogrepl.BeginMessage{})

	_, err := decoder.Accept(&pglogrepl.UpdateMessage{RelationID: 42})
	if !errors.Is(err, ErrOutboxMutation) {
		t.Fatalf("got %v, want ErrOutboxMutation", err)
	}
}

func TestDecoderRejectsOutboxTruncate(t *testing.T) {
	decoder := NewDecoder()
	_, _ = decoder.Accept(outboxRelation(42))
	_, _ = decoder.Accept(&pglogrepl.BeginMessage{})

	_, err := decoder.Accept(&pglogrepl.TruncateMessage{RelationIDs: []uint32{42}})
	if !errors.Is(err, ErrOutboxMutation) {
		t.Fatalf("got %v, want ErrOutboxMutation", err)
	}
}

func outboxRelation(id uint32) *pglogrepl.RelationMessage {
	names := []string{
		"event_id", "aggregate_type", "aggregate_id", "aggregate_version",
		"event_type", "schema_version", "occurred_at", "payload",
	}
	columns := make([]*pglogrepl.RelationMessageColumn, 0, len(names))
	for _, name := range names {
		columns = append(columns, &pglogrepl.RelationMessageColumn{Name: name})
	}
	return &pglogrepl.RelationMessage{
		RelationID: id, Namespace: "public", RelationName: "dq_outbox", Columns: columns,
	}
}

func outboxTuple() *pglogrepl.TupleData {
	values := []string{
		"event-1", "QUEUE_GENERATION", "queue-1", "1",
		"QUEUE_GENERATION_CREATED", "1", "2026-09-17 12:00:00+00", "{}",
	}
	columns := make([]*pglogrepl.TupleDataColumn, 0, len(values))
	for _, value := range values {
		columns = append(columns, &pglogrepl.TupleDataColumn{
			DataType: pglogrepl.TupleDataTypeText,
			Data:     []byte(value),
		})
	}
	return &pglogrepl.TupleData{Columns: columns}
}
