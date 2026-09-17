package application

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/indreshgahoi/distributed-queue/internal/controlplane/adapter/memory"
	"github.com/indreshgahoi/distributed-queue/internal/controlplane/domain"
)

type sequenceIDs struct{ next int }

func (source *sequenceIDs) NewID() (string, error) {
	source.next++
	return fmt.Sprintf("id-%d", source.next), nil
}

type fixedClock struct{ value time.Time }

func (clock fixedClock) Now() time.Time { return clock.value }

func TestCreateQueueCommitsCatalogAndOutboxIdempotently(t *testing.T) {
	catalog := memory.NewCatalog()
	service := NewQueueService(catalog, &sequenceIDs{}, fixedClock{value: time.Unix(100, 0)})
	request := CreateQueueRequest{
		TenantID: "tenant", QueueName: "orders", IdempotencyKey: "request-1", PartitionCount: 4,
	}

	first, err := service.CreateQueue(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.CreateQueue(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("idempotent create returned a different queue: first=%+v second=%+v", first, second)
	}
	if first.ReplicationFactor != domain.DefaultReplicationFactor || first.Lifecycle != domain.QueueProvisioning {
		t.Fatalf("defaults were not applied: %+v", first)
	}
	if events := catalog.Outbox(); len(events) != 1 || events[0].AggregateID != first.QueueID {
		t.Fatalf("catalog and outbox were not atomic: %+v", events)
	}
}

func TestCreateQueueRejectsIdempotencyConflict(t *testing.T) {
	catalog := memory.NewCatalog()
	service := NewQueueService(catalog, &sequenceIDs{}, fixedClock{value: time.Unix(100, 0)})
	_, err := service.CreateQueue(context.Background(), CreateQueueRequest{
		TenantID: "tenant", QueueName: "orders", IdempotencyKey: "same", PartitionCount: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.CreateQueue(context.Background(), CreateQueueRequest{
		TenantID: "tenant", QueueName: "other", IdempotencyKey: "same", PartitionCount: 1,
	})
	if !errors.Is(err, domain.ErrIdempotencyConflict) {
		t.Fatalf("expected idempotency conflict, got %v", err)
	}
}

func TestProjectorIsMonotonicAndIdempotent(t *testing.T) {
	projection := memory.NewProjection()
	projector := NewProjector(projection)
	event := domain.OutboxEvent{
		EventID: "event", AggregateType: "QUEUE_GENERATION", AggregateID: "queue",
		AggregateVersion: 2, EventType: "QUEUE_GENERATION_CREATED",
		SchemaVersion: domain.QueueEventSchemaVersion, Payload: []byte("new"),
	}
	applied, err := projector.Project(context.Background(), event)
	if err != nil || !applied {
		t.Fatalf("new event was not projected: applied=%v err=%v", applied, err)
	}
	applied, err = projector.Project(context.Background(), event)
	if err != nil || applied {
		t.Fatalf("duplicate event was not a no-op: applied=%v err=%v", applied, err)
	}
	older := event
	older.EventID = "older"
	older.AggregateVersion = 1
	older.Payload = []byte("old")
	applied, err = projector.Project(context.Background(), older)
	if err != nil || applied {
		t.Fatalf("older event regressed projection: applied=%v err=%v", applied, err)
	}
	version, payload, ok := projection.Get("/dq/v1/projections/current/queues/queue")
	if !ok || version != 2 || string(payload) != "new" {
		t.Fatalf("projection changed: version=%d payload=%q ok=%v", version, payload, ok)
	}
}
