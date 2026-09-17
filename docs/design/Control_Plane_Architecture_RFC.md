# System Design Document: Multi-Tenant Queue Control Plane Architecture

**Document ID:** RFC-104

**Status:** Superseded design draft. The canonical, implementation-tracked RFC
is [Control Plane Architecture RFC](../architecture/Control_Plane_Architecture_RFC.md).

**Target audience:** Staff and Principal Engineers, distributed-systems architects
**Scope:** Control Plane Reconciliation, Shard Placement Engine, Topology Management, and Consensus Metadata Evolution (ZooKeeper to etcd)

---

## 1. Architectural Philosophy: Control Plane vs. Data Plane Isolation

In a Tier-0 distributed messaging platform, the **Control Plane** (managing metadata, queue lifecycles, and partition placement) and the **Data Plane** (persisting messages, running Raft consensus, and serving reads/writes) are strictly decoupled.

```
+===================================================================================+
|                                    CLIENT LAYER                                   |
|   Admin SDK / CLI / Terraform (Control Path)       Producers / Consumers (Data Path)  |
+========================+==================================+======================+
                         |                                  |
                         v (gRPC Admin API)                 v (gRPC Data API)
+-------------------------------------------------+  +------------------------------+
| CONTROL PLANE                                   |  | DATA PLANE GATEWAYS          |
| - Stateless API Servers                         |  | - Stateless Ingress / Egress |
| - Controller Manager (Active Leader via Lease)  |  | - Local Routing Cache        |
| - Placement Engine (Bin-Packing & Topology)     |  +--------------+---------------+
| - etcd Quorum (Source of Truth for Metadata)    |                 |
+------------------------+------------------------+                 |
                         | Watch Stream                             | Forward Write
                         v                                          v
+-------------------------------------------------------------------+---------------+
| STORAGE ENGINE NODES (Multi-Raft Fleets)                                          |
|                                                                                   |
|  Node Agent (Daemon)  <--- Watches assigned partitions                            |
|       |                                                                           |
|       v                                                                           |
|  +-----------------------------------------------------------------------------+  |
|  | Multi-Raft Groups: [GroupID: 1001], [GroupID: 1002], [GroupID: 2045]...     |  |
|  | Physical Disks:   /mnt/disk00/wal ... /mnt/disk19/wal (Multiplexed WAL)     |  |
|  +-----------------------------------------------------------------------------+  |
+-----------------------------------------------------------------------------------+
```

### The Isolation Invariant
> **The Data Plane must survive the total loss of the Control Plane without dropping writes or degrading read availability.**
>
> If the entire etcd cluster and all Control Plane Controller instances crash simultaneously:
> 1. Existing queues continue to ingest and deliver messages at full throughput.
> 2. Partition leaders continue running Raft consensus locally across their peer replicas.
> 3. Gateways continue routing requests using immutable, locally cached topology maps.
> 4. Only mutating administrative operations (`CreateQueue`, `DeleteQueue`, `RebalancePartition`) are blocked until control plane quorum is restored.

---

## 2. Declarative State Model & Control Plane Components

The control plane follows a **declarative reconciliation loop** (modeled after Kubernetes operators). The system continuously drives **Observed State** to match **Desired State**.

### 2.1 Core Components

```
+-----------------------------------------------------------------------------------+
| CONTROL PLANE CLUSTER                                                             |
|                                                                                   |
|  +---------------------+                                                          |
|  | Queue API Service   | <--- Authenticates, validates schemas, enforces quotas   |
|  +----------+----------+                                                          |
|             | Writes Desired State                                                |
|             v                                                                     |
|  +---------------------+      Leader Lease      +------------------------------+  |
|  |     etcd Quorum     | <--------------------> | Queue Controller Manager     |  |
|  |  (State of Record)  |                        | (Active Leader Instance)     |  |
|  +----------+----------+                        +--------------+---------------+  |
|             |                                                  |                  |
|             | Watches Partition Assignments                    | Computes         |
|             |                                                  v Placement        |
|             |                                   +------------------------------+  |
|             |                                   | Topology & Placement Engine  |  |
|             |                                   +------------------------------+  |
+-------------|---------------------------------------------------------------------+
              |
              +=================== Streaming Watches ===================+
              |                                                         |
              v                                                         v
    +-------------------+                                     +-------------------+
    | Storage Node 01   |                                     | Storage Node 02   |
    | - Node Agent      |                                     | - Node Agent      |
    | - Multi-Raft Host |                                     | - Multi-Raft Host |
    +-------------------+                                     +-------------------+
```

1. **Queue API Gateway (Stateless):** Exposes CRUD endpoints for tenants. Validates parameters, checks billing quotas, and writes intent directly to etcd under `/desired/queues/{tenant_id}/{queue_name}`.
2. **etcd Cluster (3 or 5 nodes):** Serves as the consistent metadata store. Maintains global MVCC revisions, leases, and watch streams.
3. **Queue Controller Manager (Active-Passive):** Runs as a fleet with one active leader elected via an etcd lease (`/control/leader-lock`). It runs the continuous reconciliation loop.
4. **Placement Engine (Algorithmic Scheduler):** Evaluates hardware topology (racks, power domains, network switches) and disk metrics (IOPS, capacity) across nodes to allocate partition replicas.
5. **Node Daemon / Agent:** A lightweight process co-located on every storage host. Watches etcd for partitions assigned to its node and manages local in-process Multi-Raft instances.

---

## 3. End-to-End Lifecycle: From `CreateQueue` to `READY`

```
Client             Queue API         etcd             Controller        Node Agents       Data Gateways
  |                    |               |                  |                  |                  |
  | 1. CreateQueue()   |               |                  |                  |                  |
  |------------------->|               |                  |                  |                  |
  |                    | 2. Txn: Write |                  |                  |                  |
  |                    |    Intent     |                  |                  |                  |
  |                    |-------------->|                  |                  |                  |
  |                    |               | 3. Watch Event   |                  |                  |
  |                    |               |----------------->|                  |                  |
  |                    |               |                  | 4. Placement     |                  |
  |                    |               |                  |    Calculation   |                  |
  |                    |               | 5. Txn: Write    |                  |                  |
  |                    |               |    Assignments   |                  |                  |
  |                    |               |<-----------------|                  |                  |
  |                    |               |                  |                  |                  |
  |                    |               | 6. Watch Trigger (Assignments)      |                  |
  |                    |               |------------------------------------>|                  |
  |                    |               |                  |                  | 7. Init WAL on   |
  |                    |               |                  |                  |    disk & boot   |
  |                    |               |                  |                  |    Raft groups   |
  |                    |               |                  |                  |                  |
  |                    |               | 8. Report Replica Ready             |                  |
  |                    |               |<------------------------------------|                  |
  |                    |               |                  |                  |                  |
  |                    |               | 9. Quorum Detected                  |                  |
  |                    |               |----------------->|                  |                  |
  |                    |               | 10. Update Status: READY            |                  |
  |                    |               |<-----------------|                  |                  |
  |                    |               |                                                        |
  |                    |               | 11. Watch Trigger (Global Route Cache)                 |
  |                    |               |------------------------------------------------------->|
  |                    |               |                  |                  |                  |
  | 12. HTTP 201 "Created"             |                  |                  |                  |
  |<-------------------|               |                  |                  |                  |
```

### Detailed Execution Trace

#### Step 1: Admission Control & Quota Verification
The client calls `CreateQueue(tenant_id="tenant-42", queue_name="orders", partitions=3)`.
* API Gateway verifies the tenant's token and verifies that the tenant has not exceeded their allocated partition ceiling (e.g., maximum 50 partitions per tenant).
* Computes a global unique identifier: `QueueUID = SHA256(tenant_id + ":" + queue_name)`.

#### Step 2: Intent Persistence in etcd
The API Gateway issues an atomic transactional write to etcd:
```json
// Key: /desired/queues/tenant-42/orders
{
  "queue_uid": "q-8f1b2c3d",
  "tenant_id": "tenant-42",
  "queue_name": "orders",
  "partitions": 3,
  "status": "PROVISIONING",
  "created_at": 1726500000,
  "config": {
    "visibility_timeout_sec": 30,
    "max_receive_count": 5,
    "retention_days": 4
  }
}
```

#### Step 3: Controller Reconciliation & Placement Calculation
The active **Queue Controller Manager** receives the creation event via its continuous etcd watch stream. It invokes the **Placement Engine**:
* It queries the global cluster topology inventory (maintained in memory, synchronized from etcd).
* For each of the 3 requested partitions:
  1. Generates an immutable, cluster-wide unique 64-bit `GroupID` (e.g., `1001`, `1002`, `1003`).
  2. Selects **3 storage nodes across distinct failure domains** (e.g., Rack A, Rack B, Rack C).
  3. Selects the target NVMe disk on each node (`/mnt/disk00` through `/mnt/disk19`) using a least-loaded heuristic (evaluating current IOPS load and disk capacity).

#### Step 4: Writing Placement Declarations
The Controller writes the physical layout transactionally into etcd:
```json
// Key: /assignments/groups/1001
{
  "group_id": 1001,
  "queue_uid": "q-8f1b2c3d",
  "partition_index": 0,
  "replica_count": 3,
  "replicas": [
    { "node_id": "node-01", "disk_id": "disk-03", "peer_id": 1, "is_voter": true },
    { "node_id": "node-02", "disk_id": "disk-11", "peer_id": 2, "is_voter": true },
    { "node_id": "node-03", "disk_id": "disk-08", "peer_id": 3, "is_voter": true }
  ]
}
```

#### Step 5: Node Agent Local Provisioning
The Node Agents running on `node-01`, `node-02`, and `node-03` maintain watch streams on `/assignments/groups/`.
Upon detecting that `GroupID: 1001` is assigned to their local node:
1. **Disk Path Allocation:** `node-01` creates the partition directory on its target disk:
   `/mnt/disk03/queues/q-8f1b2c3d/partition-0/`
2. **Multiplexed WAL Registration:** Registers `GroupID: 1001` with the local disk WAL engine on `/mnt/disk03`.
3. **Raft Instance Startup:** Instantiates an embedded Raft replica (`dragonboat` or `raft-rs`).
   * **Node 01 (Seed Replica):** Configured with initial bootstrap peers `[{1, node-01}, {2, node-02}, {3, node-03}]`.
   * Node 01's Raft engine initializes term 1, counts its own vote, and broadcasts `RequestVote` or heartbeats to Node 02 and Node 03.
   * Node 02 and Node 03 start in the `FOLLOWER` state, receive heartbeats from Node 01, and persist Node 01 as the leader.

#### Step 6: Readiness Probing & State Promotion
1. Node 01's Raft leader commits an empty entry to its log (confirming that quorum is established and the WAL is actively persisting).
2. The Node Agent on Node 01 writes a heartbeat key to etcd:
   `/observed/groups/1001/status -> {"leader": "node-01", "state": "HEALTHY", "active_voters": 3}`
3. The **Queue Controller Manager** monitors the status of all 3 partitions. Once all 3 partition groups report `HEALTHY`:
   * The Controller updates `/desired/queues/tenant-42/orders/status -> READY`.
   * The Controller publishes the routing descriptor into the global routing table:
     `/routing/tables/tenant-42/orders -> { "p0": "node-01:8001", "p1": "node-04:8001", "p2": "node-07:8001" }`

#### Step 7: Gateway Ingestion Activation
Stateless Data Gateways watch the `/routing/tables/` prefix. The moment the routing entry appears, Gateways update their local read-through hash rings.
The API Gateway returns `HTTP 201 Created` to the client. The queue is now live and accepting messages.

---

## 4. Queue Node Topology & Cluster Maintenance

The Control Plane must continuously monitor the state of physical nodes, failure domains, and individual disks.

```
+-----------------------------------------------------------------------------------+
| PHYSICAL TOPOLOGY HIERARCHY                                                       |
|                                                                                   |
|  Datacenter Region                                                                |
|  └── Availability Zone (az-1)                                                     |
|      └── Server Rack (rack-104)                                                   |
|          └── Host Machine (node-01, IP: 10.0.4.11)                                |
|              ├── Controller Agent Daemon                                          |
|              └── NVMe Disk Subsystem                                              |
|                  ├── /mnt/disk00 (Multiplexed WAL Engine, 50 Groups)              |
|                  ├── /mnt/disk01 (Multiplexed WAL Engine, 50 Groups)              |
|                  └── ... up to /mnt/disk19                                        |
+-----------------------------------------------------------------------------------+
```

### 4.1 Node Registration & Ephemeral Leases

Every storage node registers its hardware capability upon boot using **etcd TTL Leases**:
1. Node Agent acquires an etcd lease with a **5-second TTL**.
2. Writes an ephemeral registration key:
   ```json
   // Key: /topology/nodes/node-01
   {
     "node_id": "node-01",
     "rack": "rack-104",
     "zone": "az-1",
     "grpc_addr": "10.0.4.11:8001",
     "disks": [
       { "id": "disk-00", "path": "/mnt/disk00", "healthy": true, "capacity_bytes": 3840000000000 },
       "... 19 other disks"
     ]
   }
   ```
3. The Node Agent sends a `LeaseKeepAlive` heartbeat every **1.5 seconds**.

---

### 4.2 Failure Scenarios & Auto-Healing Workflows

#### Failure Mode 1: Host Crash (Node Loss)
* If `node-01` crashes, its heartbeats stop. After 5 seconds, etcd revokes the lease and deletes `/topology/nodes/node-01`.
* **Immediate Data Plane Reaction (Zero Control Plane Dependency):**
  * For all Raft groups where `node-01` was Leader, followers on healthy nodes notice heartbeat loss after $150\text{--}300\text{ms}$.
  * A remaining replica (e.g., `node-02`) wins an election, becomes the new leader, and continues processing writes.
* **Control Plane Reaction (Self-Healing Reconciliation):**
  * The Controller notices `node-01` has been missing for more than a grace threshold (e.g., 60 seconds).
  * The Controller identifies all groups that had a replica on `node-01`. These groups are now operating in degraded mode (2 of 3 replicas alive).
  * For each degraded group, the Controller selects a new host (e.g., `node-09` on a distinct rack) and issues a **Safe Raft Membership Change**.

```
Single-Server Raft Membership Migration:

1. Controller -> Group Leader: AddNode(Node-09, AsLearner=true)
2. Node-09 begins receiving WAL snapshots and catches up in the background.
3. Once Node-09's replication lag is near zero:
   Controller -> Group Leader: PromoteToVoter(Node-09)
4. Group configuration safely becomes 4 nodes (Quorum = 3).
5. Controller -> Group Leader: RemoveNode(Node-01)
6. Group configuration returns to 3 nodes (Node-02, Node-03, Node-09).
```

#### Failure Mode 2: Single Disk Failure (Out of 20 Disks)
* If `/mnt/disk03` on `node-01` encounters an I/O read/write error or enters read-only mode:
* The local Node Agent marks `/mnt/disk03` as `UNHEALTHY` in its registration key.
* The Node Agent forces all Raft replicas bound to `/mnt/disk03` to shut down.
* The Controller receives the disk failure notification and triggers the migration workflow *only* for the ~50 partitions assigned to that specific disk, moving them to healthy disks on other servers. The remaining 19 disks on `node-01` continue servicing traffic unaffected.

---

## 5. Architectural Evolution: Apache ZooKeeper vs. etcd

Modern distributed architectures predominantly choose **etcd** or custom embedded consensus (like Kafka KRaft) over **ZooKeeper**. The shift was driven by protocol, API, and operational characteristics:

```
+-----------------------------------------------------------------------------------+
| ARCHITECTURAL EVOLUTION: ZOOKEEPER VS. ETCD                                       |
|                                                                                   |
|  Dimension            Apache ZooKeeper                 etcd (v3)                  |
|  ------------------   ------------------------------   -------------------------  |
|  Consensus Model      ZAB (Zookeeper Atomic Broadcast) Raft                       |
|  Data Hierarchy       Filesystem-like Znodes           Flat Key-Value with MVCC   |
|  Watch Mechanics      One-Shot Triggers (Watch Storms) Continuous Streaming Watch |
|  Session / Leases     Tied to physical TCP session     Decoupled Global Leases    |
|  Network Protocol     Custom Binary over TCP (Jute)    gRPC / Protobuf / HTTP/2   |
|  Runtime Footprint    JVM (Stop-the-world GC pauses)   Compiled Go Binary         |
+-----------------------------------------------------------------------------------+
```

### 5.1 Protocol: ZAB vs. Raft
* **ZooKeeper Atomic Broadcast (ZAB):** ZAB separates recovery mode from atomic broadcast. When a leader fails, it enters a recovery phase where it must elect a leader and synchronize history via complex epoch negotiation. ZAB is technically sound, but its specification is entangled with the JVM implementation, making clean architectural extension difficult.
* **Raft:** Raft formalizes leader election and log replication under a unified, formal set of invariants (Log Matching Property, Current-Term commit rules). This allows simpler formal verification and integration into non-JVM environments.

### 5.2 The Watch Mechanism: One-Shot Triggers vs. MVCC Streaming
* **The ZooKeeper "Watch Storm" Vulnerability:** In ZooKeeper (v3.4/3.5), watches are **one-shot triggers**. Once a znode changes, the watch fires and is deleted. If 10,000 clients are watching `/queue/leader`, all 10,000 clients receive notifications simultaneously. Each client immediately issues a new `getData()` call and attempts to re-set the watch. This causes severe network and CPU spikes that can exhaust the ZooKeeper ensemble.
* **The etcd Solution:** etcd v3 implements a **multi-version concurrency control (MVCC)** flat storage engine with **continuous revision-based streaming watches**:
  * Every write increments an immutable 64-bit cluster revision counter (`mod_revision`).
  * Clients watch keys or ranges continuously: `Watch("/assignments/", start_revision=5024)`.
  * If a client disconnects for 5 seconds, it simply reconnects and requests events starting from `last_seen_revision = 5024`. etcd streams the missed events from its internal bbolt history buffer without dropping state or overwhelming the cluster.

### 5.3 Leases and Ephemeral Nodes
* **ZooKeeper:** Ephemeral nodes exist only as long as the client's physical TCP session remains open. If a client node experiences a long garbage collection pause (e.g., 10 seconds), ZooKeeper's heartbeat timer expires, the session is terminated, and the ephemeral node is deleted. When the JVM resumes, the client believes it is still the leader, but another node has already claimed the lock.
* **etcd:** Leases are **first-class, independent resources**. A lease has a time-to-live (`TTL=5s`). A client can attach thousands of keys to a single lease. The lease heartbeat is maintained over gRPC keep-alive streams. Furthermore, etcd exposes monotonic fencing tokens via `CreateRevision` directly on the lease key, allowing downstream storage systems to definitively reject zombie writes.

---

## 6. Principal Engineer System Design: Implementation Specifications

### 6.1 Data Model Specifications

```go
package controlplane

// QueueMetadata represents the declarative configuration of a queue.
type QueueMetadata struct {
	QueueUID       string            `json:"queue_uid"`
	TenantID       string            `json:"tenant_id"`
	QueueName      string            `json:"queue_name"`
	Partitions     int               `json:"partitions"`
	Status         QueueStatus       `json:"status"`
	MaxReceive     int               `json:"max_receive"`
	VisibilitySec  int               `json:"visibility_sec"`
	CreatedAt      int64             `json:"created_at"`
	ModRevision    int64             `json:"mod_revision"`
}

type QueueStatus string

const (
	StatusProvisioning QueueStatus = "PROVISIONING"
	StatusReady        QueueStatus = "READY"
	StatusDegraded     QueueStatus = "DEGRADED"
	StatusDeleting     QueueStatus = "DELETING"
)

// PartitionAssignment represents physical placement across nodes and NVMe drives.
type PartitionAssignment struct {
	GroupID        uint64            `json:"group_id"`
	QueueUID       string            `json:"queue_uid"`
	PartitionIndex int               `json:"partition_index"`
	Replicas       []ReplicaLocation `json:"replicas"`
}

type ReplicaLocation struct {
	NodeID   string `json:"node_id"`
	DiskID   string `json:"disk_id"`   // e.g., "disk-03" -> /mnt/disk03
	PeerID   uint64 `json:"peer_id"`   // Raft Peer ID within the group
	IsVoter  bool   `json:"is_voter"`  // Voter vs. Learner/Witness
}

// NodeRegistration represents the hardware state reported by each storage machine.
type NodeRegistration struct {
	NodeID        string       `json:"node_id"`
	RackID        string       `json:"rack_id"`
	ZoneID        string       `json:"zone_id"`
	GRPCAddress   string       `json:"grpc_address"`
	Disks         []DiskStatus `json:"disks"`
	LastHeartbeat int64        `json:"last_heartbeat"`
}

type DiskStatus struct {
	DiskID        string `json:"disk_id"`
	MountPath     string `json:"mount_path"` // e.g. /mnt/disk00
	IsHealthy     bool   `json:"is_healthy"`
	TotalBytes    uint64 `json:"total_bytes"`
	UsedBytes     uint64 `json:"used_bytes"`
	ActiveGroups  int    `json:"active_groups"`
}
```

---

### 6.2 Placement Engine Algorithm (Failure-Domain Aware Bin-Packing)

```go
package controlplane

import (
	"errors"
	"sort"
	"sync"
)

type PlacementEngine struct {
	mu    sync.RWMutex
	nodes map[string]*NodeRegistration
}

func NewPlacementEngine() *PlacementEngine {
	return &PlacementEngine{
		nodes: make(map[string]*NodeRegistration),
	}
}

// AllocateReplicas selects N distinct nodes and disks satisfying rack anti-affinity and load balancing.
func (pe *PlacementEngine) AllocateReplicas(replicaCount int) ([]ReplicaLocation, error) {
	pe.mu.RLock()
	defer pe.mu.RUnlock()

	if len(pe.nodes) < replicaCount {
		return nil, errors.New("insufficient physical nodes to satisfy replication factor")
	}

	// 1. Group nodes by Rack to satisfy rack anti-affinity
	racks := make(map[string][]*NodeRegistration)
	for _, node := range pe.nodes {
		racks[node.RackID] = append(racks[node.RackID], node)
	}

	if len(racks) < replicaCount {
		return nil, errors.New("insufficient unique racks: cannot satisfy failure-domain isolation")
	}

	var selectedReplicas []ReplicaLocation
	usedRacks := make(map[string]bool)

	// Sort racks deterministically or pick least-loaded rack
	for rackID, candidateNodes := range racks {
		if usedRacks[rackID] {
			continue
		}

		// 2. Within the rack, select the node with the lowest global partition count
		selectedNode := pe.selectLeastLoadedNode(candidateNodes)
		if selectedNode == nil {
			continue
		}

		// 3. Within the selected node, pick the physical disk with the fewest active groups
		selectedDisk, err := pe.selectBestDisk(selectedNode)
		if err != nil {
			continue
		}

		selectedReplicas = append(selectedReplicas, ReplicaLocation{
			NodeID:  selectedNode.NodeID,
			DiskID:  selectedDisk.DiskID,
			PeerID:  uint64(len(selectedReplicas) + 1),
			IsVoter: true,
		})

		usedRacks[rackID] = true
		if len(selectedReplicas) == replicaCount {
			break
		}
	}

	if len(selectedReplicas) < replicaCount {
		return nil, errors.New("placement engine failed to find sufficient valid replica candidates")
	}

	return selectedReplicas, nil
}

func (pe *PlacementEngine) selectLeastLoadedNode(nodes []*NodeRegistration) *NodeRegistration {
	sort.Slice(nodes, func(i, j int) bool {
		return pe.totalNodeGroups(nodes[i]) < pe.totalNodeGroups(nodes[j])
	})
	return nodes[0]
}

func (pe *PlacementEngine) totalNodeGroups(n *NodeRegistration) int {
	total := 0
	for _, d := range n.Disks {
		if d.IsHealthy {
			total += d.ActiveGroups
		}
	}
	return total
}

func (pe *PlacementEngine) selectBestDisk(node *NodeRegistration) (*DiskStatus, error) {
	var candidates []DiskStatus
	for _, d := range node.Disks {
		if d.IsHealthy {
			candidates = append(candidates, d)
		}
	}
	if len(candidates) == 0 {
		return nil, errors.New("no healthy disks on node")
	}

	// Pick disk with fewest active groups (least I/O contention on its multiplexed WAL)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].ActiveGroups < candidates[j].ActiveGroups
	})

	return &candidates[0], nil
}
```

---

### 6.3 Storage Node Agent (Watching etcd and Instantiating Raft)

```go
package nodeagent

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type NodeAgent struct {
	nodeID     string
	etcdCli    *clientv3.Client
	raftHost   RaftManager // Local Multi-Raft Host Controller (Dragonboat/raft-rs wrapper)
}

func (na *NodeAgent) StartWatch(ctx context.Context) {
	watchPrefix := fmt.Sprintf("/assignments/nodes/%s/", na.nodeID)
	watchChan := na.etcdCli.Watch(ctx, watchPrefix, clientv3.WithPrefix())

	for resp := range watchChan {
		for _, ev := range resp.Events {
			switch ev.Type {
			case clientv3.EventTypePut:
				var assign PartitionAssignment
				if err := json.Unmarshal(ev.Kv.Value, &assign); err != nil {
					log.Printf("invalid assignment payload: %v", err)
					continue
				}
				na.reconcilePartition(ctx, assign)

			case clientv3.EventTypeDelete:
				// Teardown / decommission local partition instance
				groupID := parseGroupIDFromKey(string(ev.Kv.Key))
				na.raftHost.StopReplica(groupID)
			}
		}
	}
}

func (na *NodeAgent) reconcilePartition(ctx context.Context, assign PartitionAssignment) {
	// Find local disk mapping
	var localDiskID string
	var localPeerID uint64
	for _, rep := range assign.Replicas {
		if rep.NodeID == na.nodeID {
			localDiskID = rep.DiskID
			localPeerID = rep.PeerID
			break
		}
	}

	mountPath := fmt.Sprintf("/mnt/%s/wal", localDiskID)

	// Initialize partition on local multiplexed WAL engine and start Raft replica
	err := na.raftHost.StartReplica(
		assign.GroupID,
		localPeerID,
		assign.Replicas,
		mountPath,
	)
	if err != nil {
		log.Printf("failed to start replica for group %d: %v", assign.GroupID, err)
		return
	}

	// Report healthy status back to etcd
	statusKey := fmt.Sprintf("/observed/groups/%d/%s", assign.GroupID, na.nodeID)
	na.etcdCli.Put(ctx, statusKey, `{"status": "BOOTED"}`)
}
```

---

## 7. Principal Engineer Failure Modes & Pushback Playbook

### Scenario 1: The Active Controller Split-Brain
* **Interviewer Challenge:** "What happens if the active Queue Controller Manager suffers a 20-second Garbage Collection pause? A standby Controller claims the leader lock and both start making placement decisions simultaneously."
* **Principal Defense:** "We enforce **Monotonic Fencing on Metadata Writes**:
  1. Leadership is claimed using an etcd lease.
  2. When Controller B takes over, etcd assigns a higher monotonic revision (`mod_revision`) to `/control/leader-lock`.
  3. Every transaction committed to etcd by a Controller includes a precondition:
     `clientv3.Compare(clientv3.ModRevision("/control/leader-lock"), "=", myLeaderRevision)`
  4. When Controller A wakes up from its GC pause and attempts to execute its buffered placement decision, etcd evaluates the precondition, sees that the leadership revision has changed, and **fails the transaction atomically**. Zero split-brain decisions can be persisted."

### Scenario 2: Cascading Node Failures (Thundering Herd Auto-Healing)
* **Interviewer Challenge:** "If a network switch blips and 50 nodes disconnect for 6 seconds, does the Control Plane immediately trigger 50,000 partition re-replications and take down your cluster?"
* **Principal Defense:** "No. We deploy **Multi-Tier Hysteresis and Rate-Limiting**:
  1. **Two-Stage Failure Detection:** A node heartbeat timeout (5s) marks the node as *Temporarily Unreachable*, not *Dead*. Partition re-replication is suppressed for a grace window of 5 minutes.
  2. **Fast-Path In-Place Reconnect:** If the node reconnects within 5 minutes, it resumes participating in its existing Raft groups using its on-disk WAL with no data copied over the network.
  3. **Global Migration Rate Limiting:** Even after 5 minutes, the Controller limits concurrent active partition migrations to a fixed global ceiling (e.g., maximum 50 concurrent migrations cluster-wide) to protect inter-rack network bandwidth."

### Scenario 3: Asymmetric Partitioning of the Control Plane
* **Interviewer Challenge:** "What happens if storage nodes can talk to each other to replicate messages, but cannot reach the etcd cluster?"
* **Principal Defense:** "Data Plane writes continue uninterrupted. Because storage nodes host their own in-process Raft consensus groups, they elect leaders, replicate to local NVMe disks, and serve client writes completely independent of etcd. Only control path operations (e.g., creating a brand new queue or resizing partitions) will pause until the control plane network partition resolves."

---

## 8. Summary Architecture Matrix

| Dimension | Control Plane (Administrative Layer) | Data Plane (High-Throughput Storage) |
| :--- | :--- | :--- |
| **Consensus Engine** | External etcd cluster (3 or 5 dedicated nodes) | Embedded Multi-Raft (`Dragonboat` / `tikv/raft-rs`) |
| **Throughput Target** | $\sim 100	ext{--}1,000$ operations/sec (CRUD) | $\sim 1,000,000+$ messages/sec |
| **Latency Budget** | $10	ext{--}50	ext{ ms}$ ($P_{99}$) | $< 2	ext{--}5	ext{ ms}$ ($P_{99}$) |
| **Storage Architecture**| bbolt (B+ Tree with MVCC copy-on-write) | Multiplexed append-only sequential WAL per NVMe disk |
| **State Lifespan** | Long-lived configuration records | Destructive message lifecycle (Enqueue $	o$ Lease $	o$ Delete) |
