package consensus

import (
	"context"

	"github.com/indreshgahoi/distributed-queue/internal/dataplane/domain"
)

// Group is the project-owned boundary around a partition consensus group.
// Propose returns only after the implementation's commit, durability, and apply
// contract has completed.
type Group interface {
	Lineage() domain.Lineage
	Propose(context.Context, domain.Command) (domain.Result, error)
	NextReady(context.Context) (domain.Message, bool, error)
	DueTransitions(context.Context, int64, int) ([]domain.Message, []domain.Message, error)
}
