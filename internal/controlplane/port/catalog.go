package port

import (
	"context"

	"github.com/indreshgahoi/distributed-queue/internal/controlplane/domain"
)

// Catalog commits the queue aggregate, idempotency record, and outbox event in
// one atomic transaction. Implementations must never expose only a subset.
type Catalog interface {
	CreateQueue(context.Context, domain.CreateQueue) (domain.QueueGeneration, domain.OutboxEvent, error)
}

type Projection interface {
	PutIfNewer(context.Context, string, uint64, []byte) (bool, error)
}
