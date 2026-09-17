package application

import (
	"errors"
	"testing"

	"github.com/indreshgahoi/distributed-queue/internal/controlplane/domain"
)

func TestAllocateReplicasUsesDistinctRacksAndStableLeastLoadOrder(t *testing.T) {
	nodes := []domain.Node{
		node("node-c", "rack-c", 3, volume("nvme-1", 3)),
		node("node-a", "rack-a", 0, volume("nvme-2", 2), volume("nvme-1", 0)),
		node("node-b", "rack-b", 1, volume("nvme-1", 1)),
	}
	placements, err := AllocateReplicas(nodes, 3, PlacementPolicy{
		StorageClass: "local-nvme", RequiredFreeBytes: 100, RequireDistinctRack: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if placements[0].NodeID != "node-a" || placements[0].VolumeID != "nvme-1" ||
		placements[1].NodeID != "node-b" || placements[2].NodeID != "node-c" {
		t.Fatalf("unexpected deterministic placement: %+v", placements)
	}
}

func TestAllocateReplicasRejectsInsufficientFailureDomains(t *testing.T) {
	nodes := []domain.Node{
		node("node-a", "rack-a", 0, volume("nvme-1", 0)),
		node("node-b", "rack-a", 0, volume("nvme-1", 0)),
		node("node-c", "rack-b", 0, volume("nvme-1", 0)),
	}
	_, err := AllocateReplicas(nodes, 3, PlacementPolicy{
		StorageClass: "local-nvme", RequiredFreeBytes: 100, RequireDistinctRack: true,
	})
	if !errors.Is(err, domain.ErrInsufficientCapacity) {
		t.Fatalf("expected capacity error, got %v", err)
	}
}

func node(id, rack string, groups int, volumes ...domain.Volume) domain.Node {
	return domain.Node{
		NodeID: id, Rack: rack, SessionAlive: true, ActiveGroups: groups, Volumes: volumes,
	}
}

func volume(id string, groups int) domain.Volume {
	return domain.Volume{
		VolumeID: id, StorageClass: "local-nvme", Healthy: true,
		CapacityBytes: 1_000, ReservedBytes: 0, ActiveGroups: groups,
	}
}
