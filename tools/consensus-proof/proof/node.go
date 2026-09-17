package proof

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/logger"
)

const (
	DeploymentID uint64 = 29001
	ShardID      uint64 = 1001
)

type Replica struct {
	ID      uint64
	Address string
	Root    string
	Host    *dragonboat.NodeHost
}

type replicaOptions struct {
	snapshotEntries uint64
}

func init() {
	// Keep proof output focused on assertion and benchmark results. Dragonboat
	// otherwise emits an INFO line for every snapshot and lifecycle transition.
	for _, name := range []string{"dragonboat", "config", "logdb", "raft", "rsm", "transport"} {
		logger.GetLogger(name).SetLevel(logger.ERROR)
	}
}

func StartReplica(id uint64, address, root string, members map[uint64]dragonboat.Target) (*Replica, error) {
	return startReplica(id, address, root, members, replicaOptions{snapshotEntries: 10})
}

func startReplica(
	id uint64,
	address string,
	root string,
	members map[uint64]dragonboat.Target,
	options replicaOptions,
) (*Replica, error) {
	expert := config.GetDefaultExpertConfig()
	expert.LogDB = config.GetTinyMemLogDBConfig()
	nodeHostConfig := config.NodeHostConfig{
		DeploymentID:   DeploymentID,
		WALDir:         filepath.Join(root, "wal"),
		NodeHostDir:    filepath.Join(root, "nodehost"),
		RTTMillisecond: 5,
		RaftAddress:    address,
		NotifyCommit:   true,
		Expert:         expert,
	}
	host, err := dragonboat.NewNodeHost(nodeHostConfig)
	if err != nil {
		return nil, fmt.Errorf("create node host: %w", err)
	}
	replicaConfig := config.Config{
		ReplicaID:           id,
		ShardID:             ShardID,
		CheckQuorum:         true,
		PreVote:             true,
		ElectionRTT:         20,
		HeartbeatRTT:        2,
		SnapshotEntries:     options.snapshotEntries,
		CompactionOverhead:  5,
		OrderedConfigChange: true,
		MaxInMemLogSize:     16 << 20,
	}
	if err := host.StartReplica(members, false, NewStateMachine, replicaConfig); err != nil {
		host.Close()
		return nil, fmt.Errorf("start replica: %w", err)
	}
	return &Replica{ID: id, Address: address, Root: root, Host: host}, nil
}

func RestartReplica(id uint64, address, root string) (*Replica, error) {
	return StartReplica(id, address, root, map[uint64]dragonboat.Target{})
}

func (r *Replica) Propose(ctx context.Context, delta uint64) (uint64, error) {
	result, err := r.Host.SyncPropose(ctx, r.Host.GetNoOPSession(ShardID), EncodeDelta(delta))
	if err != nil {
		return 0, err
	}
	return result.Value, nil
}

func (r *Replica) Read(ctx context.Context) (State, error) {
	value, err := r.Host.SyncRead(ctx, ShardID, nil)
	if err != nil {
		return State{}, err
	}
	state, ok := value.(State)
	if !ok {
		return State{}, fmt.Errorf("unexpected lookup result %T", value)
	}
	return state, nil
}

func (r *Replica) Snapshot(ctx context.Context) (uint64, error) {
	return r.Host.SyncRequestSnapshot(ctx, ShardID, dragonboat.DefaultSnapshotOption)
}

func (r *Replica) Close() { r.Host.Close() }

func WaitForLeader(replicas []*Replica, timeout time.Duration) (*Replica, uint64, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, replica := range replicas {
			leaderID, term, valid, err := replica.Host.GetLeaderID(ShardID)
			if err != nil || !valid || leaderID == 0 {
				continue
			}
			for _, candidate := range replicas {
				if candidate.ID == leaderID {
					return candidate, term, nil
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return nil, 0, fmt.Errorf("leader not elected within %s", timeout)
}
