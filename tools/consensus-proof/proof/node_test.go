package proof

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4"
)

func TestThreeReplicaProposalCompletesAfterApply(t *testing.T) {
	replicas := startCluster(t, 3)
	leader, _, err := WaitForLeader(replicas, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	value, err := leader.Propose(ctx, 7)
	if err != nil {
		t.Fatalf("propose: %v", err)
	}
	if value != 7 {
		t.Fatalf("proposal result = %d, want 7", value)
	}

	for _, replica := range replicas {
		state, err := readEventually(replica, 10*time.Second)
		if err != nil {
			t.Fatalf("read replica %d: %v", replica.ID, err)
		}
		if state.Value != 7 || state.LastIndex == 0 {
			t.Fatalf("replica %d state = %+v", replica.ID, state)
		}
	}
}

func readEventually(replica *Replica, timeout time.Duration) (State, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		state, err := replica.Read(ctx)
		cancel()
		if err == nil {
			return state, nil
		}
		lastErr = err
		time.Sleep(20 * time.Millisecond)
	}
	return State{}, lastErr
}

func TestSnapshotAndRestartRecoverAppliedState(t *testing.T) {
	address := freeAddress(t)
	root := filepath.Join(t.TempDir(), "replica-1")
	members := map[uint64]dragonboat.Target{1: dragonboat.Target(address)}
	replica, err := StartReplica(1, address, root, members)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := WaitForLeader([]*Replica{replica}, 10*time.Second); err != nil {
		replica.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := 0; i < 25; i++ {
		if _, err := replica.Propose(ctx, 1); err != nil {
			replica.Close()
			t.Fatalf("proposal %d: %v", i, err)
		}
	}
	if _, err := replica.Snapshot(ctx); err != nil {
		replica.Close()
		t.Fatalf("snapshot: %v", err)
	}
	replica.Close()

	restarted, err := RestartReplica(1, address, root)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if _, _, err := WaitForLeader([]*Replica{restarted}, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	state, err := restarted.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state.Value != 25 || state.LastIndex == 0 {
		t.Fatalf("recovered state = %+v, want value 25", state)
	}
}

func TestProposalDoesNotSucceedWithoutQuorum(t *testing.T) {
	replicas := startCluster(t, 3)
	leader, _, err := WaitForLeader(replicas, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for _, replica := range replicas {
		if replica.ID == leader.ID {
			continue
		}
		if err := replica.Host.StopReplica(ShardID, replica.ID); err != nil {
			t.Fatalf("stop replica %d: %v", replica.ID, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 750*time.Millisecond)
	defer cancel()
	if _, err := leader.Propose(ctx, 1); err == nil {
		t.Fatal("proposal succeeded after quorum was removed")
	}
}

func BenchmarkThreeReplicaSyncProposal(b *testing.B) {
	// Automatic snapshots are disabled so this benchmark isolates the durable
	// replicated proposal path. Snapshot cost is a separate storage concern.
	replicas := startClusterWithOptions(b, 3, replicaOptions{})
	leader, _, err := WaitForLeader(replicas, 10*time.Second)
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := leader.Propose(ctx, 1); err != nil {
			b.Fatal(err)
		}
	}
}

func startCluster(t testing.TB, count int) []*Replica {
	t.Helper()
	return startClusterWithOptions(t, count, replicaOptions{snapshotEntries: 10})
}

func startClusterWithOptions(t testing.TB, count int, options replicaOptions) []*Replica {
	t.Helper()
	addresses := make([]string, count)
	members := make(map[uint64]dragonboat.Target, count)
	for i := range addresses {
		addresses[i] = freeAddress(t)
		members[uint64(i+1)] = dragonboat.Target(addresses[i])
	}
	replicas := make([]*Replica, 0, count)
	for i, address := range addresses {
		replica, err := startReplica(
			uint64(i+1),
			address,
			filepath.Join(t.TempDir(), "replica"),
			members,
			options,
		)
		if err != nil {
			for _, started := range replicas {
				started.Close()
			}
			t.Fatal(err)
		}
		replicas = append(replicas, replica)
	}
	t.Cleanup(func() {
		for _, replica := range replicas {
			replica.Close()
		}
	})
	return replicas
}

func freeAddress(t testing.TB) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}
