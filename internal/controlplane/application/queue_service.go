package application

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/indreshgahoi/distributed-queue/internal/controlplane/domain"
	"github.com/indreshgahoi/distributed-queue/internal/controlplane/port"
)

type IDSource interface {
	NewID() (string, error)
}

type RandomIDSource struct{}

func (RandomIDSource) NewID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}

type Clock interface{ Now() time.Time }

type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now().UTC() }

type QueueService struct {
	catalog port.Catalog
	ids     IDSource
	clock   Clock
}

func NewQueueService(catalog port.Catalog, ids IDSource, clock Clock) *QueueService {
	return &QueueService{catalog: catalog, ids: ids, clock: clock}
}

type CreateQueueRequest struct {
	TenantID          string
	QueueName         string
	IdempotencyKey    string
	PartitionCount    uint32
	ReplicationFactor uint32
}

func (s *QueueService) CreateQueue(ctx context.Context, request CreateQueueRequest) (domain.QueueGeneration, error) {
	if request.ReplicationFactor == 0 {
		request.ReplicationFactor = domain.DefaultReplicationFactor
	}
	if request.TenantID == "" || request.QueueName == "" || request.IdempotencyKey == "" ||
		request.PartitionCount == 0 || request.ReplicationFactor == 0 {
		return domain.QueueGeneration{}, domain.ErrInvalidQueue
	}
	queueID, err := s.ids.NewID()
	if err != nil {
		return domain.QueueGeneration{}, fmt.Errorf("allocate queue id: %w", err)
	}
	generationID, err := s.ids.NewID()
	if err != nil {
		return domain.QueueGeneration{}, fmt.Errorf("allocate generation id: %w", err)
	}
	routingSeed, err := s.ids.NewID()
	if err != nil {
		return domain.QueueGeneration{}, fmt.Errorf("allocate routing seed: %w", err)
	}
	eventID, err := s.ids.NewID()
	if err != nil {
		return domain.QueueGeneration{}, fmt.Errorf("allocate event id: %w", err)
	}
	now := s.clock.Now().UTC()
	command := domain.CreateQueue{
		TenantID: request.TenantID, QueueName: request.QueueName,
		IdempotencyKey: request.IdempotencyKey, PartitionCount: request.PartitionCount,
		ReplicationFactor: request.ReplicationFactor, RequestHash: requestHash(request),
		QueueID: queueID, GenerationID: generationID, RoutingSeed: routingSeed, EventID: eventID, Now: now,
	}
	queue, _, err := s.catalog.CreateQueue(ctx, command)
	return queue, err
}

func requestHash(request CreateQueueRequest) string {
	canonical := fmt.Sprintf("%s\x00%s\x00%d\x00%d", request.TenantID, request.QueueName, request.PartitionCount, request.ReplicationFactor)
	digest := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(digest[:])
}
