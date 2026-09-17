package pgcdc

import (
	"context"
	"fmt"

	"github.com/indreshgahoi/distributed-queue/internal/controlplane/application"
	"github.com/jackc/pglogrepl"
)

type Acknowledger interface {
	Acknowledge(context.Context, pglogrepl.LSN) error
}

type Processor struct {
	projector    *application.Projector
	acknowledger Acknowledger
}

func NewProcessor(projector *application.Projector, acknowledger Acknowledger) *Processor {
	return &Processor{projector: projector, acknowledger: acknowledger}
}

// Process projects every event from one committed PostgreSQL transaction
// before acknowledging its end LSN. A crash after projection and before the
// acknowledgement safely replays events because projection writes are
// monotonic and idempotent.
func (processor *Processor) Process(ctx context.Context, transaction Transaction) error {
	if transaction.EndLSN == 0 {
		return fmt.Errorf("transaction end LSN is required")
	}
	for _, event := range transaction.Events {
		if _, err := processor.projector.Project(ctx, event); err != nil {
			return fmt.Errorf("project event %s: %w", event.EventID, err)
		}
	}
	if err := processor.acknowledger.Acknowledge(ctx, transaction.EndLSN); err != nil {
		return fmt.Errorf("acknowledge transaction LSN %s: %w", transaction.EndLSN, err)
	}
	return nil
}
