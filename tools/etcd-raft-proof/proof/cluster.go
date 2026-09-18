package proof

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const maxDrainRounds = 10_000

// Cluster is a deterministic, in-process harness for evaluating the etcd/raft
// core. It deliberately models the application-owned Ready contract: persist
// entries and hard state first, then send messages, then apply committed data.
// It is not a production transport or durable storage implementation.
type Cluster struct {
	nodes map[uint64]*node
	ids   []uint64
}

type node struct {
	id           uint64
	raw          *raft.RawNode
	storage      *raft.MemoryStorage
	active       bool
	applied      []string
	appliedIndex uint64
	confState    *pb.ConfState
}

type outbound struct {
	message *pb.Message
}

// NewCluster bootstraps one local RawNode for every supplied replica ID.
func NewCluster(ids ...uint64) (*Cluster, error) {
	if len(ids) == 0 {
		return nil, errors.New("at least one replica is required")
	}

	sortedIDs := append([]uint64(nil), ids...)
	sort.Slice(sortedIDs, func(i, j int) bool { return sortedIDs[i] < sortedIDs[j] })
	peers := make([]raft.Peer, 0, len(sortedIDs))
	for _, id := range sortedIDs {
		peers = append(peers, raft.Peer{ID: id})
	}

	cluster := &Cluster{
		nodes: make(map[uint64]*node, len(sortedIDs)),
		ids:   sortedIDs,
	}
	for _, id := range sortedIDs {
		storage := raft.NewMemoryStorage()
		raw, err := raft.NewRawNode(newConfig(id, storage, 0))
		if err != nil {
			return nil, fmt.Errorf("create replica %d: %w", id, err)
		}
		if err := raw.Bootstrap(peers); err != nil {
			return nil, fmt.Errorf("bootstrap replica %d: %w", id, err)
		}
		cluster.nodes[id] = &node{
			id:        id,
			raw:       raw,
			storage:   storage,
			active:    true,
			confState: &pb.ConfState{},
		}
	}
	if err := cluster.Drain(); err != nil {
		return nil, err
	}
	return cluster, nil
}

func newConfig(id uint64, storage raft.Storage, applied uint64) *raft.Config {
	return &raft.Config{
		ID:                        id,
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   storage,
		Applied:                   applied,
		MaxSizePerMsg:             1 << 20,
		MaxCommittedSizePerReady:  1 << 20,
		MaxUncommittedEntriesSize: 4 << 20,
		MaxInflightMsgs:           256,
		MaxInflightBytes:          4 << 20,
		CheckQuorum:               true,
		PreVote:                   true,
	}
}

// Campaign asks a replica to campaign and drains all resulting work.
func (c *Cluster) Campaign(id uint64) error {
	n, ok := c.nodes[id]
	if !ok {
		return fmt.Errorf("unknown replica %d", id)
	}
	if !n.active {
		return fmt.Errorf("replica %d is stopped", id)
	}
	if err := n.raw.Campaign(); err != nil {
		return fmt.Errorf("campaign replica %d: %w", id, err)
	}
	return c.Drain()
}

// Propose submits bytes to the current leader and drains the simulated network.
// A nil return means only that the core accepted the proposal. Callers must wait
// for application of the corresponding committed entry before acknowledging it.
func (c *Cluster) Propose(data string) error {
	leader, ok := c.Leader()
	if !ok {
		return errors.New("no active leader")
	}
	if err := c.nodes[leader].raw.Propose([]byte(data)); err != nil {
		return fmt.Errorf("propose through replica %d: %w", leader, err)
	}
	return c.Drain()
}

// Leader returns the active replica that currently reports the leader role.
func (c *Cluster) Leader() (uint64, bool) {
	for _, id := range c.ids {
		n := c.nodes[id]
		if n.active && n.raw.Status().RaftState == raft.StateLeader {
			return id, true
		}
	}
	return 0, false
}

// SetActive simulates process or network loss. Messages to inactive replicas
// are dropped, matching the failure model used by the deterministic proof.
func (c *Cluster) SetActive(id uint64, active bool) error {
	n, ok := c.nodes[id]
	if !ok {
		return fmt.Errorf("unknown replica %d", id)
	}
	n.active = active
	return nil
}

// Applied returns a defensive copy of one replica's applied normal entries.
func (c *Cluster) Applied(id uint64) []string {
	n := c.nodes[id]
	return append([]string(nil), n.applied...)
}

// SnapshotAndRestart checkpoints every active replica, compacts through the
// checkpoint, and reconstructs its RawNode from Storage plus application state.
func (c *Cluster) SnapshotAndRestart() error {
	for _, id := range c.ids {
		n := c.nodes[id]
		if !n.active {
			continue
		}
		data, err := json.Marshal(n.applied)
		if err != nil {
			return fmt.Errorf("encode replica %d snapshot: %w", id, err)
		}
		if _, err := n.storage.CreateSnapshot(n.appliedIndex, n.confState, data); err != nil {
			return fmt.Errorf("create replica %d snapshot: %w", id, err)
		}
		if err := n.storage.Compact(n.appliedIndex); err != nil {
			return fmt.Errorf("compact replica %d: %w", id, err)
		}

		snapshot, err := n.storage.Snapshot()
		if err != nil {
			return fmt.Errorf("read replica %d snapshot: %w", id, err)
		}
		var restored []string
		if err := json.Unmarshal(snapshot.GetData(), &restored); err != nil {
			return fmt.Errorf("restore replica %d application snapshot: %w", id, err)
		}
		raw, err := raft.NewRawNode(newConfig(id, n.storage, snapshot.GetMetadata().GetIndex()))
		if err != nil {
			return fmt.Errorf("restart replica %d: %w", id, err)
		}
		n.raw = raw
		n.applied = restored
		n.appliedIndex = snapshot.GetMetadata().GetIndex()
		n.confState = proto.Clone(snapshot.GetMetadata().GetConfState()).(*pb.ConfState)
	}
	return c.Drain()
}

// Drain handles Ready batches until no active replica has more work. The order
// is the critical contract an etcd/raft host must preserve in production.
func (c *Cluster) Drain() error {
	for round := 0; round < maxDrainRounds; round++ {
		progressed := false
		messages := make([]outbound, 0)

		for _, id := range c.ids {
			n := c.nodes[id]
			if !n.active || !n.raw.HasReady() {
				continue
			}
			progressed = true
			rd := n.raw.Ready()
			if err := persistReady(n, rd); err != nil {
				return err
			}
			for _, message := range rd.Messages {
				messages = append(messages, outbound{message: message})
			}
			if err := applyCommitted(n, rd.CommittedEntries); err != nil {
				return err
			}
			n.raw.Advance(rd)
		}

		for _, item := range messages {
			target, ok := c.nodes[item.message.GetTo()]
			if !ok || !target.active {
				continue
			}
			if err := target.raw.Step(item.message); err != nil && !errors.Is(err, raft.ErrStepPeerNotFound) {
				return fmt.Errorf("deliver %s from %d to %d: %w", item.message.GetType(), item.message.GetFrom(), item.message.GetTo(), err)
			}
			progressed = true
		}

		if !progressed {
			return nil
		}
	}
	return errors.New("raft harness did not quiesce")
}

func persistReady(n *node, rd raft.Ready) error {
	if !raft.IsEmptySnap(rd.Snapshot) {
		if err := n.storage.ApplySnapshot(rd.Snapshot); err != nil {
			return fmt.Errorf("persist replica %d snapshot: %w", n.id, err)
		}
	}
	if !raft.IsEmptyHardState(rd.HardState) {
		if err := n.storage.SetHardState(rd.HardState); err != nil {
			return fmt.Errorf("persist replica %d hard state: %w", n.id, err)
		}
	}
	if err := n.storage.Append(rd.Entries); err != nil {
		return fmt.Errorf("persist replica %d entries: %w", n.id, err)
	}
	return nil
}

func applyCommitted(n *node, entries []*pb.Entry) error {
	for _, entry := range entries {
		switch entry.GetType() {
		case pb.EntryConfChange:
			change := &pb.ConfChange{}
			if err := proto.Unmarshal(entry.GetData(), change); err != nil {
				return fmt.Errorf("decode replica %d configuration entry: %w", n.id, err)
			}
			n.confState = n.raw.ApplyConfChange(change)
		case pb.EntryConfChangeV2:
			change := &pb.ConfChangeV2{}
			if err := proto.Unmarshal(entry.GetData(), change); err != nil {
				return fmt.Errorf("decode replica %d joint configuration entry: %w", n.id, err)
			}
			n.confState = n.raw.ApplyConfChange(change)
		case pb.EntryNormal:
			if len(entry.GetData()) > 0 {
				n.applied = append(n.applied, string(entry.GetData()))
			}
		}
		n.appliedIndex = entry.GetIndex()
	}
	return nil
}
