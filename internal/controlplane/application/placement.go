package application

import (
	"sort"

	"github.com/indreshgahoi/distributed-queue/internal/controlplane/domain"
)

type PlacementPolicy struct {
	StorageClass        string
	RequiredFreeBytes   int64
	RequireDistinctRack bool
}

type placementCandidate struct {
	node   domain.Node
	volume domain.Volume
}

func AllocateReplicas(nodes []domain.Node, replicationFactor uint32, policy PlacementPolicy) ([]domain.ReplicaPlacement, error) {
	if replicationFactor == 0 || policy.StorageClass == "" || policy.RequiredFreeBytes < 0 {
		return nil, domain.ErrInvalidQueue
	}
	candidates := make([]placementCandidate, 0, len(nodes))
	for _, node := range nodes {
		if !node.SessionAlive || node.Draining || node.NodeID == "" {
			continue
		}
		volume, ok := bestVolume(node.Volumes, policy)
		if !ok {
			continue
		}
		candidates = append(candidates, placementCandidate{node: node, volume: volume})
	}
	sort.Slice(candidates, func(i, j int) bool {
		leftLoad := candidates[i].node.ActiveGroups + candidates[i].volume.ActiveGroups
		rightLoad := candidates[j].node.ActiveGroups + candidates[j].volume.ActiveGroups
		if leftLoad != rightLoad {
			return leftLoad < rightLoad
		}
		if candidates[i].node.Rack != candidates[j].node.Rack {
			return candidates[i].node.Rack < candidates[j].node.Rack
		}
		return candidates[i].node.NodeID < candidates[j].node.NodeID
	})

	placements := make([]domain.ReplicaPlacement, 0, replicationFactor)
	usedNodes := make(map[string]bool)
	usedRacks := make(map[string]bool)
	for _, candidate := range candidates {
		if usedNodes[candidate.node.NodeID] {
			continue
		}
		if policy.RequireDistinctRack && (candidate.node.Rack == "" || usedRacks[candidate.node.Rack]) {
			continue
		}
		placements = append(placements, domain.ReplicaPlacement{
			NodeID: candidate.node.NodeID, VolumeID: candidate.volume.VolumeID,
			Ordinal: uint32(len(placements)),
		})
		usedNodes[candidate.node.NodeID] = true
		usedRacks[candidate.node.Rack] = true
		if uint32(len(placements)) == replicationFactor {
			return placements, nil
		}
	}
	return nil, domain.ErrInsufficientCapacity
}

func bestVolume(volumes []domain.Volume, policy PlacementPolicy) (domain.Volume, bool) {
	eligible := make([]domain.Volume, 0, len(volumes))
	for _, volume := range volumes {
		free := volume.CapacityBytes - volume.ReservedBytes
		if volume.Healthy && volume.StorageClass == policy.StorageClass && free >= policy.RequiredFreeBytes {
			eligible = append(eligible, volume)
		}
	}
	if len(eligible) == 0 {
		return domain.Volume{}, false
	}
	sort.Slice(eligible, func(i, j int) bool {
		if eligible[i].ActiveGroups != eligible[j].ActiveGroups {
			return eligible[i].ActiveGroups < eligible[j].ActiveGroups
		}
		return eligible[i].VolumeID < eligible[j].VolumeID
	})
	return eligible[0], true
}
