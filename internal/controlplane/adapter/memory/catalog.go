package memory

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/indreshgahoi/distributed-queue/internal/controlplane/domain"
)

type requestRecord struct {
	hash  string
	queue domain.QueueGeneration
	event domain.OutboxEvent
}

// Catalog is a transactional in-memory adapter used by domain tests and local
// development. PostgreSQL replaces it in a deployed control plane.
type Catalog struct {
	mu       sync.Mutex
	requests map[string]requestRecord
	liveName map[string]string
	queues   map[string]domain.QueueGeneration
	outbox   []domain.OutboxEvent
}

func NewCatalog() *Catalog {
	return &Catalog{
		requests: make(map[string]requestRecord),
		liveName: make(map[string]string),
		queues:   make(map[string]domain.QueueGeneration),
	}
}

func (c *Catalog) CreateQueue(_ context.Context, command domain.CreateQueue) (domain.QueueGeneration, domain.OutboxEvent, error) {
	if !command.Valid() {
		return domain.QueueGeneration{}, domain.OutboxEvent{}, domain.ErrInvalidQueue
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	requestKey := command.TenantID + "\x00" + command.IdempotencyKey
	if previous, ok := c.requests[requestKey]; ok {
		if previous.hash != command.RequestHash {
			return domain.QueueGeneration{}, domain.OutboxEvent{}, domain.ErrIdempotencyConflict
		}
		return previous.queue, previous.event, nil
	}
	nameKey := command.TenantID + "\x00" + command.QueueName
	if _, exists := c.liveName[nameKey]; exists {
		return domain.QueueGeneration{}, domain.OutboxEvent{}, domain.ErrQueueExists
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
		SchemaVersion: domain.QueueEventSchemaVersion, OccurredAt: command.Now,
		Payload: payload,
	}

	// These mutations are one critical section to model the production
	// transaction: callers can observe all three records or none of them.
	c.queues[queue.QueueID] = queue
	c.liveName[nameKey] = queue.QueueID
	c.outbox = append(c.outbox, event)
	c.requests[requestKey] = requestRecord{hash: command.RequestHash, queue: queue, event: event}
	return queue, event, nil
}

func (c *Catalog) Outbox() []domain.OutboxEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]domain.OutboxEvent(nil), c.outbox...)
}

type projectionValue struct {
	version uint64
	payload []byte
}

type Projection struct {
	mu     sync.Mutex
	values map[string]projectionValue
}

func NewProjection() *Projection {
	return &Projection{values: make(map[string]projectionValue)}
}

func (p *Projection) PutIfNewer(_ context.Context, key string, version uint64, payload []byte) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	current, ok := p.values[key]
	if ok && current.version >= version {
		return false, nil
	}
	p.values[key] = projectionValue{version: version, payload: append([]byte(nil), payload...)}
	return true, nil
}

func (p *Projection) Get(key string) (uint64, []byte, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	value, ok := p.values[key]
	return value.version, append([]byte(nil), value.payload...), ok
}
