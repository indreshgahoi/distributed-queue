package proof

import (
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"testing"

	"go.etcd.io/raft/v3"
)

func TestMain(m *testing.M) {
	raft.SetLogger(&raft.DefaultLogger{Logger: log.New(io.Discard, "", 0)})
	os.Exit(m.Run())
}

func TestCommittedProposalConvergesOnThreeReplicas(t *testing.T) {
	cluster := newElectedCluster(t)

	if err := cluster.Propose("publish:order-42"); err != nil {
		t.Fatalf("propose: %v", err)
	}

	want := []string{"publish:order-42"}
	for _, id := range []uint64{1, 2, 3} {
		if got := cluster.Applied(id); !slices.Equal(got, want) {
			t.Fatalf("replica %d applied %v, want %v", id, got, want)
		}
	}
}

func TestProposalDoesNotCommitWithoutQuorum(t *testing.T) {
	cluster := newElectedCluster(t)
	leader, ok := cluster.Leader()
	if !ok {
		t.Fatal("leader not found")
	}
	for _, id := range []uint64{1, 2, 3} {
		if id != leader {
			if err := cluster.SetActive(id, false); err != nil {
				t.Fatal(err)
			}
		}
	}

	// RawNode.Propose returning nil is not a commit acknowledgement. The host
	// must correlate the command with a later committed-and-applied entry.
	if err := cluster.Propose("publish:must-not-commit"); err != nil {
		t.Fatalf("proposal submission: %v", err)
	}
	if got := cluster.Applied(leader); len(got) != 0 {
		t.Fatalf("leader applied without quorum: %v", got)
	}
}

func TestSnapshotAndRetainedRaftStateRecoverThenContinue(t *testing.T) {
	cluster := newElectedCluster(t)
	for i := 0; i < 5; i++ {
		if err := cluster.Propose(fmt.Sprintf("publish:%d", i)); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}
	if err := cluster.SnapshotAndRestart(); err != nil {
		t.Fatalf("snapshot and restart: %v", err)
	}
	if err := cluster.Campaign(2); err != nil {
		t.Fatalf("campaign after restart: %v", err)
	}
	if err := cluster.Propose("publish:after-restart"); err != nil {
		t.Fatalf("propose after restart: %v", err)
	}

	want := []string{
		"publish:0",
		"publish:1",
		"publish:2",
		"publish:3",
		"publish:4",
		"publish:after-restart",
	}
	for _, id := range []uint64{1, 2, 3} {
		if got := cluster.Applied(id); !slices.Equal(got, want) {
			t.Fatalf("replica %d restored %v, want %v", id, got, want)
		}
	}
}

func BenchmarkCreateOneThousandLocalRawNodes(b *testing.B) {
	for range b.N {
		groups := make([]*Cluster, 0, 1_000)
		for groupID := 0; groupID < 1_000; groupID++ {
			cluster, err := NewCluster(1)
			if err != nil {
				b.Fatal(err)
			}
			groups = append(groups, cluster)
		}
		if len(groups) != 1_000 {
			b.Fatalf("created %d groups", len(groups))
		}
	}
}

func newElectedCluster(t *testing.T) *Cluster {
	t.Helper()
	cluster, err := NewCluster(1, 2, 3)
	if err != nil {
		t.Fatalf("new cluster: %v", err)
	}
	if err := cluster.Campaign(1); err != nil {
		t.Fatalf("campaign: %v", err)
	}
	if _, ok := cluster.Leader(); !ok {
		t.Fatal("leader not elected")
	}
	return cluster
}
