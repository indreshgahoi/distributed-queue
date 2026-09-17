package proof

import (
	"encoding/binary"
	"errors"
	"io"

	sm "github.com/lni/dragonboat/v4/statemachine"
)

const snapshotSize = 16

// State is the smallest deterministic state needed to test Dragonboat's
// commit, apply, snapshot, and restart contracts without importing Dragonboat
// into the production queue domain.
type State struct {
	Value     uint64
	LastIndex uint64
}

// StateMachine is deliberately independent of queue semantics. G1 evaluates
// the consensus dependency; G2 will bind the approved dependency to the queue
// state machine behind the project-owned consensus.Group port.
type StateMachine struct {
	state State
}

func NewStateMachine(uint64, uint64) sm.IStateMachine {
	return &StateMachine{}
}

func (s *StateMachine) Update(entry sm.Entry) (sm.Result, error) {
	if len(entry.Cmd) != 8 {
		return sm.Result{}, errors.New("proposal must contain one uint64 delta")
	}
	s.state.Value += binary.BigEndian.Uint64(entry.Cmd)
	s.state.LastIndex = entry.Index
	return sm.Result{Value: s.state.Value}, nil
}

func (s *StateMachine) Lookup(interface{}) (interface{}, error) {
	return s.state, nil
}

func (s *StateMachine) SaveSnapshot(
	w io.Writer,
	_ sm.ISnapshotFileCollection,
	done <-chan struct{},
) error {
	select {
	case <-done:
		return sm.ErrSnapshotStopped
	default:
	}
	var data [snapshotSize]byte
	binary.BigEndian.PutUint64(data[0:8], s.state.Value)
	binary.BigEndian.PutUint64(data[8:16], s.state.LastIndex)
	_, err := w.Write(data[:])
	return err
}

func (s *StateMachine) RecoverFromSnapshot(
	r io.Reader,
	_ []sm.SnapshotFile,
	done <-chan struct{},
) error {
	select {
	case <-done:
		return sm.ErrSnapshotStopped
	default:
	}
	var data [snapshotSize]byte
	if _, err := io.ReadFull(r, data[:]); err != nil {
		return err
	}
	s.state.Value = binary.BigEndian.Uint64(data[0:8])
	s.state.LastIndex = binary.BigEndian.Uint64(data[8:16])
	return nil
}

func (s *StateMachine) Close() error { return nil }

func EncodeDelta(delta uint64) []byte {
	var data [8]byte
	binary.BigEndian.PutUint64(data[:], delta)
	return data[:]
}
