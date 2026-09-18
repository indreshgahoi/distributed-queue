package queuegroup

import (
	"encoding/json"
	"errors"
	"io"

	"github.com/indreshgahoi/distributed-queue/internal/dataplane/domain"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

type queryKind uint8

const (
	queryNextReady queryKind = iota + 1
	queryDueTransitions
	queryState
)

type query struct {
	kind  queryKind
	now   int64
	limit int
}

type dueTransitions struct {
	delayed []domain.Message
	leases  []domain.Message
}

type state struct {
	lastAppliedIndex uint64
	retainedMessages int
	retainedBytes    int64
}

// queueStateMachine adapts the deterministic queue domain to Dragonboat. Raft
// log indexes are not reused as queue command indexes: Raft also commits
// membership and internal entries, while the domain requires a gap-free index
// over queue commands only.
type queueStateMachine struct {
	lineage domain.Lineage
	config  domain.Configuration
	machine *domain.StateMachine
}

func createQueueStateMachine(lineage domain.Lineage, config domain.Configuration) sm.CreateStateMachineFunc {
	return func(uint64, uint64) sm.IStateMachine {
		machine, err := domain.NewStateMachine(lineage, config)
		if err != nil {
			panic(err)
		}
		return &queueStateMachine{lineage: lineage, config: config, machine: machine}
	}
}

func (s *queueStateMachine) Update(entry sm.Entry) (sm.Result, error) {
	var command domain.Command
	if err := json.Unmarshal(entry.Cmd, &command); err != nil {
		return sm.Result{}, err
	}
	result, err := s.machine.Apply(s.machine.LastAppliedIndex()+1, command)
	if err != nil {
		return sm.Result{}, err
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return sm.Result{}, err
	}
	return sm.Result{Value: s.machine.LastAppliedIndex(), Data: encoded}, nil
}

func (s *queueStateMachine) Lookup(value interface{}) (interface{}, error) {
	request, ok := value.(query)
	if !ok {
		return nil, errors.New("unsupported queue state-machine query")
	}
	switch request.kind {
	case queryNextReady:
		message, found := s.machine.NextReady()
		return nextReady{message: message, found: found}, nil
	case queryDueTransitions:
		leases := s.machine.DueLeases(request.now, request.limit)
		remaining := request.limit - len(leases)
		if remaining < 0 {
			remaining = 0
		}
		return dueTransitions{
			delayed: s.machine.DueDelayed(request.now, remaining),
			leases:  leases,
		}, nil
	case queryState:
		return state{
			lastAppliedIndex: s.machine.LastAppliedIndex(),
			retainedMessages: s.machine.RetainedMessages(),
			retainedBytes:    s.machine.RetainedBytes(),
		}, nil
	default:
		return nil, errors.New("unknown queue state-machine query")
	}
}

func (s *queueStateMachine) SaveSnapshot(
	w io.Writer,
	_ sm.ISnapshotFileCollection,
	done <-chan struct{},
) error {
	select {
	case <-done:
		return sm.ErrSnapshotStopped
	default:
	}
	// Dragonboat stores the authoritative included Raft term in its snapshot
	// metadata. The domain snapshot keeps zero here rather than inventing a term
	// that the IStateMachine callback does not receive.
	return json.NewEncoder(w).Encode(s.machine.CaptureSnapshot(0))
}

func (s *queueStateMachine) RecoverFromSnapshot(
	r io.Reader,
	_ []sm.SnapshotFile,
	done <-chan struct{},
) error {
	select {
	case <-done:
		return sm.ErrSnapshotStopped
	default:
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	machine, _, err := domain.RestoreSnapshot(data, s.config, s.lineage)
	if err != nil {
		return err
	}
	s.machine = machine
	return nil
}

func (s *queueStateMachine) Close() error { return nil }

type nextReady struct {
	message domain.Message
	found   bool
}
