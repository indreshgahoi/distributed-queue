package queuegroup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/indreshgahoi/distributed-queue/internal/dataplane/consensus"
	"github.com/indreshgahoi/distributed-queue/internal/dataplane/domain"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/logger"
)

const defaultDeploymentID uint64 = 29002

var _ consensus.Group = (*Group)(nil)

type Config struct {
	DeploymentID   uint64
	ReplicaID      uint64
	Address        string
	StorageRoot    string
	InitialMembers map[uint64]string
	Lineage        domain.Lineage
	Queue          domain.Configuration
	RequestTimeout time.Duration
	SnapshotEvery  uint64
}

type Group struct {
	host           *dragonboat.NodeHost
	lineage        domain.Lineage
	replicaID      uint64
	requestTimeout time.Duration
	closeOnce      sync.Once
}

type Stats struct {
	LastAppliedIndex uint64
	RetainedMessages int
	RetainedBytes    int64
}

func init() {
	for _, name := range []string{"dragonboat", "config", "logdb", "raft", "rsm", "transport"} {
		logger.GetLogger(name).SetLevel(logger.CRITICAL)
	}
}

func Start(configure Config) (*Group, error) {
	if err := validateConfig(configure); err != nil {
		return nil, err
	}
	deploymentID := configure.DeploymentID
	if deploymentID == 0 {
		deploymentID = defaultDeploymentID
	}
	expert := config.GetDefaultExpertConfig()
	expert.LogDB = config.GetTinyMemLogDBConfig()
	host, err := dragonboat.NewNodeHost(config.NodeHostConfig{
		DeploymentID:   deploymentID,
		WALDir:         filepath.Join(configure.StorageRoot, "wal"),
		NodeHostDir:    filepath.Join(configure.StorageRoot, "nodehost"),
		RTTMillisecond: 5,
		RaftAddress:    configure.Address,
		NotifyCommit:   true,
		Expert:         expert,
	})
	if err != nil {
		return nil, fmt.Errorf("create Dragonboat node host: %w", err)
	}
	members := make(map[uint64]dragonboat.Target, len(configure.InitialMembers))
	for replicaID, address := range configure.InitialMembers {
		members[replicaID] = dragonboat.Target(address)
	}
	err = host.StartReplica(
		members,
		false,
		createQueueStateMachine(configure.Lineage, configure.Queue),
		config.Config{
			ReplicaID:           configure.ReplicaID,
			ShardID:             configure.Lineage.RaftGroupID,
			CheckQuorum:         true,
			PreVote:             true,
			ElectionRTT:         20,
			HeartbeatRTT:        2,
			SnapshotEntries:     configure.SnapshotEvery,
			CompactionOverhead:  5,
			OrderedConfigChange: true,
			MaxInMemLogSize:     16 << 20,
		},
	)
	if err != nil {
		host.Close()
		return nil, fmt.Errorf("start Dragonboat replica: %w", err)
	}
	return &Group{
		host:           host,
		lineage:        configure.Lineage,
		replicaID:      configure.ReplicaID,
		requestTimeout: configure.RequestTimeout,
	}, nil
}

func (g *Group) Lineage() domain.Lineage { return g.lineage }

func (g *Group) Propose(ctx context.Context, command domain.Command) (domain.Result, error) {
	// Domain errors returned from a Raft state-machine callback are fatal to
	// Dragonboat. Reject envelope errors before replication; payload validation
	// remains deterministic apply logic and becomes a replicated REJECTED result.
	if command.SchemaVersion != domain.CommandVersion {
		return domain.Result{}, domain.ErrUnsupportedSchema
	}
	if command.CommandID == "" {
		return domain.Result{}, domain.ErrInvalidCommand
	}
	if command.Lineage != g.lineage {
		return domain.Result{}, domain.ErrLineageMismatch
	}
	encoded, err := json.Marshal(command)
	if err != nil {
		return domain.Result{}, fmt.Errorf("encode queue command: %w", err)
	}
	operationContext, cancel := g.operationContext(ctx)
	defer cancel()
	result, err := g.host.SyncPropose(
		operationContext,
		g.host.GetNoOPSession(g.lineage.RaftGroupID),
		encoded,
	)
	if err != nil {
		return domain.Result{}, err
	}
	var applied domain.Result
	if err := json.Unmarshal(result.Data, &applied); err != nil {
		return domain.Result{}, fmt.Errorf("decode applied queue result: %w", err)
	}
	return applied, nil
}

func (g *Group) NextReady(ctx context.Context) (domain.Message, bool, error) {
	value, err := g.read(ctx, query{kind: queryNextReady})
	if err != nil {
		return domain.Message{}, false, err
	}
	result, ok := value.(nextReady)
	if !ok {
		return domain.Message{}, false, fmt.Errorf("unexpected next-ready result %T", value)
	}
	return result.message, result.found, nil
}

func (g *Group) DueTransitions(
	ctx context.Context,
	now int64,
	limit int,
) ([]domain.Message, []domain.Message, error) {
	value, err := g.read(ctx, query{kind: queryDueTransitions, now: now, limit: limit})
	if err != nil {
		return nil, nil, err
	}
	result, ok := value.(dueTransitions)
	if !ok {
		return nil, nil, fmt.Errorf("unexpected due-transitions result %T", value)
	}
	return result.delayed, result.leases, nil
}

func (g *Group) Stats(ctx context.Context) (Stats, error) {
	value, err := g.read(ctx, query{kind: queryState})
	if err != nil {
		return Stats{}, err
	}
	result, ok := value.(state)
	if !ok {
		return Stats{}, fmt.Errorf("unexpected state result %T", value)
	}
	return Stats{
		LastAppliedIndex: result.lastAppliedIndex,
		RetainedMessages: result.retainedMessages,
		RetainedBytes:    result.retainedBytes,
	}, nil
}

// LocalStats is an observability and convergence probe. It deliberately uses
// a stale local read and must never be used to authorize a customer operation.
func (g *Group) LocalStats() (Stats, error) {
	value, err := g.host.StaleRead(g.lineage.RaftGroupID, query{kind: queryState})
	if err != nil {
		return Stats{}, err
	}
	result, ok := value.(state)
	if !ok {
		return Stats{}, fmt.Errorf("unexpected local state result %T", value)
	}
	return Stats{
		LastAppliedIndex: result.lastAppliedIndex,
		RetainedMessages: result.retainedMessages,
		RetainedBytes:    result.retainedBytes,
	}, nil
}

func (g *Group) RequestSnapshot(ctx context.Context) (uint64, error) {
	operationContext, cancel := g.operationContext(ctx)
	defer cancel()
	return g.host.SyncRequestSnapshot(
		operationContext,
		g.lineage.RaftGroupID,
		dragonboat.DefaultSnapshotOption,
	)
}

func (g *Group) Leader() (uint64, uint64, bool, error) {
	return g.host.GetLeaderID(g.lineage.RaftGroupID)
}

func (g *Group) ReplicaID() uint64 { return g.replicaID }

func (g *Group) Close() {
	g.closeOnce.Do(g.host.Close)
}

func WaitForLeader(groups []*Group, timeout time.Duration) (*Group, uint64, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, group := range groups {
			leaderID, term, valid, err := group.Leader()
			if err != nil || !valid || leaderID == 0 {
				continue
			}
			for _, candidate := range groups {
				if candidate.replicaID == leaderID {
					ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
					_, readErr := candidate.Stats(ctx)
					cancel()
					if readErr == nil {
						return candidate, term, nil
					}
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return nil, 0, fmt.Errorf("leader not elected within %s", timeout)
}

func (g *Group) read(ctx context.Context, request query) (interface{}, error) {
	operationContext, cancel := g.operationContext(ctx)
	defer cancel()
	return g.host.SyncRead(operationContext, g.lineage.RaftGroupID, request)
}

func (g *Group) operationContext(parent context.Context) (context.Context, context.CancelFunc) {
	if _, hasDeadline := parent.Deadline(); hasDeadline {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, g.requestTimeout)
}

func validateConfig(configure Config) error {
	if configure.ReplicaID == 0 || configure.Address == "" || configure.StorageRoot == "" ||
		configure.Lineage.RaftGroupID == 0 || configure.RequestTimeout <= 0 {
		return errors.New("invalid replicated queue group configuration")
	}
	if _, err := domain.NewStateMachine(configure.Lineage, configure.Queue); err != nil {
		return fmt.Errorf("invalid queue state machine configuration: %w", err)
	}
	for replicaID, address := range configure.InitialMembers {
		if replicaID == 0 || address == "" {
			return errors.New("invalid initial replica membership")
		}
	}
	return nil
}
