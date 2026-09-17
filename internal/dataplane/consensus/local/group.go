package local

import (
	"context"
	"sync"

	"github.com/indreshgahoi/distributed-queue/internal/dataplane/domain"
)

// Group is a single-process adapter for semantic tests and local development.
// It provides no replicated durability and must not be used to claim distributed
// guarantees.
type Group struct {
	mu      sync.Mutex
	lineage domain.Lineage
	machine *domain.StateMachine
	next    uint64
}

func NewGroup(lineage domain.Lineage, machine *domain.StateMachine) *Group {
	return &Group{lineage: lineage, machine: machine, next: machine.LastAppliedIndex() + 1}
}

func (g *Group) Lineage() domain.Lineage { return g.lineage }

func (g *Group) Propose(ctx context.Context, command domain.Command) (domain.Result, error) {
	if err := ctx.Err(); err != nil {
		return domain.Result{}, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	result, err := g.machine.Apply(g.next, command)
	if err == nil {
		g.next++
	}
	return result, err
}

func (g *Group) NextReady(ctx context.Context) (domain.Message, bool, error) {
	if err := ctx.Err(); err != nil {
		return domain.Message{}, false, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	message, ok := g.machine.NextReady()
	return message, ok, nil
}

func (g *Group) DueTransitions(ctx context.Context, now int64, limit int) ([]domain.Message, []domain.Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	leases := g.machine.DueLeases(now, limit)
	remaining := limit - len(leases)
	if remaining < 0 {
		remaining = 0
	}
	delayed := g.machine.DueDelayed(now, remaining)
	return delayed, leases, nil
}
