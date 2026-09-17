package pgcdc

import (
	"context"
	"errors"
	"testing"

	"github.com/indreshgahoi/distributed-queue/internal/controlplane/adapter/memory"
	"github.com/indreshgahoi/distributed-queue/internal/controlplane/application"
	"github.com/indreshgahoi/distributed-queue/internal/controlplane/domain"
	"github.com/jackc/pglogrepl"
)

func TestProcessorAcknowledgesOnlyAfterEveryEventProjects(t *testing.T) {
	projection := memory.NewProjection()
	acknowledger := &recordingAcknowledger{}
	processor := NewProcessor(application.NewProjector(projection), acknowledger)
	transaction := Transaction{
		EndLSN: 100,
		Events: []domain.OutboxEvent{
			queueEvent("queue-1", 1),
			queueEvent("queue-2", 1),
		},
	}

	if err := processor.Process(context.Background(), transaction); err != nil {
		t.Fatalf("process: %v", err)
	}
	if acknowledger.lsn != 100 {
		t.Fatalf("acknowledged LSN = %d, want 100", acknowledger.lsn)
	}
	for _, queueID := range []string{"queue-1", "queue-2"} {
		if _, _, ok := projection.Get("/dq/v1/projections/current/queues/" + queueID); !ok {
			t.Fatalf("queue %s was not projected", queueID)
		}
	}
}

func TestProcessorDoesNotAcknowledgeUnsupportedEvent(t *testing.T) {
	projection := memory.NewProjection()
	acknowledger := &recordingAcknowledger{}
	processor := NewProcessor(application.NewProjector(projection), acknowledger)
	event := queueEvent("queue-1", 1)
	event.SchemaVersion = 99

	err := processor.Process(context.Background(), Transaction{EndLSN: 100, Events: []domain.OutboxEvent{event}})
	if !errors.Is(err, domain.ErrUnsupportedEvent) {
		t.Fatalf("got %v, want ErrUnsupportedEvent", err)
	}
	if acknowledger.lsn != 0 {
		t.Fatalf("acknowledged LSN = %d, want zero", acknowledger.lsn)
	}
}

type recordingAcknowledger struct{ lsn pglogrepl.LSN }

func (acknowledger *recordingAcknowledger) Acknowledge(_ context.Context, lsn pglogrepl.LSN) error {
	acknowledger.lsn = lsn
	return nil
}

func queueEvent(queueID string, version uint64) domain.OutboxEvent {
	return domain.OutboxEvent{
		EventID: "event-" + queueID, AggregateType: "QUEUE_GENERATION", AggregateID: queueID,
		AggregateVersion: version, EventType: "QUEUE_GENERATION_CREATED",
		SchemaVersion: domain.QueueEventSchemaVersion, Payload: []byte("{}"),
	}
}
