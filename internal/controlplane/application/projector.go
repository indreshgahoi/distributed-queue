package application

import (
	"context"
	"fmt"

	"github.com/indreshgahoi/distributed-queue/internal/controlplane/domain"
	"github.com/indreshgahoi/distributed-queue/internal/controlplane/port"
)

type Projector struct{ projection port.Projection }

func NewProjector(projection port.Projection) *Projector {
	return &Projector{projection: projection}
}

func (p *Projector) Project(ctx context.Context, event domain.OutboxEvent) (bool, error) {
	if event.SchemaVersion != domain.QueueEventSchemaVersion ||
		event.AggregateType != "QUEUE_GENERATION" ||
		event.EventType != "QUEUE_GENERATION_CREATED" ||
		event.AggregateID == "" || event.AggregateVersion == 0 || len(event.Payload) == 0 {
		return false, domain.ErrUnsupportedEvent
	}
	key := fmt.Sprintf("/dq/v1/projections/current/queues/%s", event.AggregateID)
	return p.projection.PutIfNewer(ctx, key, event.AggregateVersion, event.Payload)
}
