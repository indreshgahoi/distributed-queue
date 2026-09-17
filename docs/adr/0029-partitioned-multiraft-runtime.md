# ADR 0029: Use One Raft Group per Queue Partition

## Status

Proposed for review. No implementation is authorized by this ADR yet.

## Context

The project now has the foundations of a replicated queue but not a complete
distributed authority model. It has durable logical indexes and terms,
bounded follower transport, durable follower append, immutable initial replica
membership, and local snapshots. It does not yet have node-coordinated leader
election, majority commit, committed-only state-machine application, safe
membership changes, or multi-partition customer queues.

The existing `LocalMessageQueue` combines two responsibilities:

```text
queue state machine
        +
local WAL durability authority
```

That composition is correct for a local queue. It is not correct for a Raft
replica because a distributed command must have one authoritative history. If
Raft first commits a command and `LocalMessageQueue` then writes an independent
WAL record, the system has two logs whose success, recovery, and compaction can
disagree.

The current queue node also has one `storageRoot`. It cannot intentionally
place replicas across several local disks, preserve a stable replica-to-volume
binding, or schedule disk work fairly across tenants. Creating one queue-node
process per disk would incorrectly present one physical host as several
independent failure domains.

The target must support:

- many tenants and queues;
- multiple immutable partitions per queue generation;
- one independently available replica group per partition;
- many Raft groups hosted by one queue-node process;
- three replicas by default on distinct physical hosts;
- majority-durable acknowledgement;
- stable placement on one of many local storage volumes;
- continued service by established groups during a metadata outage when a
  voting majority remains available.

## Decision

### Partition is the consensus boundary

Each queue-generation partition is one independent Raft group:

```text
RaftGroupIdentity = queueId + generationId + partitionId
```

The generation ID prevents storage or protocol messages from a deleted queue
incarnation from joining a new group with the same customer-visible name.

Queue ordering, commit index, state-machine application, snapshot boundary,
leader term, and membership transitions are partition scoped. There is no
queue-wide consensus group and no global total order across partitions.

### Use a shared Multi-Raft runtime

One queue-node process hosts many partition groups. Groups share transport,
timers, executors, metrics, and disk scheduling. A group must not own a Java
thread, scheduler, HTTP client, or connection pool merely because it exists.

```text
QueueNode
  -> shared Multi-Raft runtime
       -> partition group A
       -> partition group B
       -> partition group C
       -> ...
```

Per-group state remains isolated even though execution resources are shared.
A corrupt, blocked, or unavailable partition must not poison unrelated groups.

### Prefer Apache Ratis over a new consensus implementation

Use Apache Ratis as the proposed Raft protocol implementation. Ratis owns:

- terms and durable votes;
- election timers and leader election;
- AppendEntries and follower progress;
- majority commit-index advancement;
- Raft membership transitions;
- Raft protocol transport;
- snapshot installation protocol.

The queue project owns:

- tenant and queue catalog;
- partition selection and customer routing;
- desired placement and failure-domain policy;
- queue commands and deterministic queue state;
- delivery leases, retries, ACK, NACK, and DLQ semantics;
- admission control and tenant fairness;
- local volume selection and operational lifecycle;
- customer-visible durability guarantees.

This dependency choice is conditional on a proof of concept. The ADR cannot be
accepted until the validation gates below succeed. The exact Ratis version and
dependency surface are deliberately not fixed before that review.

### Raft log is distributed message-history authority

The distributed runtime has one authoritative Raft log. A committed command is
applied directly to a deterministic queue state machine; the state machine does
not append the command to a second WAL.

```text
client command
    -> Raft log
    -> majority commit
    -> deterministic QueueStateMachine.apply(command)
    -> response
```

The existing segmented WAL remains valuable for standalone mode, tests, and as
reference storage code. It may be adapted behind Ratis only if the proof of
concept demonstrates complete compatibility with Ratis log truncation,
purging, snapshots, recovery, and durability callbacks. It must not remain an
independent second authority.

### PostgreSQL remains desired-state authority only

PostgreSQL continues to own tenant identity, queue configuration, desired
placement, provisioning workflow, and administrative lifecycle. It does not
own `currentTerm`, `votedFor`, `leaderId`, `commitIndex`, or `lastApplied`.

Once a group is formed, its current leader and commit authority come from its
Raft quorum. Loss of PostgreSQL may prevent queue creation, deletion,
replacement, or membership intent changes, but must not by itself stop a
healthy established group that retains quorum.

Metadata registration leases therefore cease to be data-plane serving
authority for established Raft groups. They remain control-plane liveness and
placement signals.

### Separate state-machine logic from local durability

Extract deterministic queue transitions from `LocalMessageQueue` into a state
machine that performs no filesystem, network, election, or wall-clock authority
operation.

```text
QueueStateMachine
  apply(Publish)
  apply(StartDelivery)
  apply(Acknowledge)
  apply(NegativeAcknowledge)
  apply(ExpireDelivery)
  apply(MoveToDeadLetter)
```

Standalone local execution may wrap this state machine with the existing WAL.
Distributed execution applies only committed Raft commands.

### Make storage volumes explicit

A queue node is one failure-domain member that manages several named storage
volumes. One local partition replica has one stable volume binding. Adding,
removing, or reordering configured disks must not silently remap existing
replicas.

Replica placement spreads voting members across hosts before considering local
disk selection. Separate disks on one host are not separate voting failure
domains.

Partition-local storage remains the first implementation. A shared per-volume
journal and cross-partition group commit are separate performance decisions
that require benchmarks and a failure-isolation ADR.

## Consequences

### Positive

- The consensus boundary matches the ordering and failover boundary.
- Independent partitions can elect, commit, recover, and fail independently.
- Established groups no longer require PostgreSQL for each message or leader
  decision.
- Ratis avoids inventing election, voting, term, and membership machinery after
  the project has already learned the underlying concepts.
- Queue semantics remain project-owned and independently testable.
- Shared Multi-Raft resources allow one process to host many idle groups.
- Explicit volumes make multi-disk capacity visible without pretending disks
  are separate hosts.
- The authority chain has one distributed log rather than two competing WALs.

### Negative

- Apache Ratis becomes a critical runtime and compatibility dependency.
- Existing WAL, snapshot, and hard-state abstractions cannot be connected
  mechanically; their authority roles must be redesigned.
- `LocalMessageQueue` requires a substantial responsibility split.
- Queue behavior must become deterministic under replay and leadership change.
- Thousands of groups create timer, memory, file-descriptor, metric-cardinality,
  and startup-recovery pressure even without one thread per group.
- Multi-volume lifecycle, disk drain, and capacity reporting add operational
  state that does not exist today.
- Multi-partition receive weakens queue-wide empty observations and global
  ordering unless the customer selects a one-partition queue.

## Alternatives considered

### Implement custom Raft

Rejected as the default direction. It provides maximum learning and control,
but election safety, pre-vote, leader transfer, joint consensus, snapshot
installation, log repair, and operational hardening would become the main
product. The repository's differentiating work should now be durable queue
semantics and multi-tenant operation.

A minimal custom implementation may still be maintained as an isolated test or
teaching module, but it must not become production authority by accident.

### Continue with fixed bootstrap leaders

Rejected. It can demonstrate majority append but cannot safely survive leader
failure, network partition, delayed processes, or conflicting histories.

### Use PostgreSQL, etcd, or ZooKeeper for every leader decision

Rejected. A generic coordination lease does not prove that a leader contains
all committed queue entries. Putting the control-plane quorum in every message
decision also couples unrelated partitions and makes metadata availability part
of message commit.

### Use Apache BookKeeper as the storage tier

Deferred, not rejected permanently. BookKeeper provides scalable replicated
ledgers and efficient storage-node journals, but it changes the architecture
from node-owned partition replicas to a separate bookie storage service. Queue
leadership, state-machine semantics, partition routing, and administrative
metadata still require design. This is a larger operational pivot than using a
Raft library inside the current queue-node model.

### Keep one Raft group per customer queue

Rejected. One hot queue would remain limited by one serial log, and the whole
queue would share one failure and election boundary. Partition count would not
increase write parallelism.

### Use one Raft group for the whole cluster

Rejected. It creates a global ordering bottleneck and couples every tenant and
partition to one quorum.

### Treat every disk as a queue node

Rejected. Disks on one host share process, kernel, power, network, and host
failure. Presenting them as independent voting nodes would make replication
factor claims unsafe.

### Introduce a shared per-disk journal immediately

Deferred. It may amortize force operations across sparse partitions, but it
also couples recovery and reclamation across tenants and increases the failure
blast radius. Start with explicit volumes and partition-local logs, measure the
result, and make group commit a separate decision.

## Proof-of-concept acceptance gates

Before changing production queue behavior, the Ratis spike must demonstrate:

1. one JVM hosting at least 1,000 idle groups without one thread per group;
2. three local processes electing one leader and committing a command through a
   majority;
3. restart recovery without applying a committed command twice;
4. follower outage while the remaining majority continues;
5. old leader isolation and rejection after a higher term is observed;
6. snapshot creation and follower snapshot installation;
7. deterministic mapping from storage lineage to Ratis group identity;
8. explicit storage-directory selection for one of several configured volumes;
9. bounded executors, pending bytes, and replication requests;
10. metrics whose labels do not create one unbounded time series per tenant or
    message;
11. dependency, license, security, and Java 21 compatibility review;
12. benchmark comparison against the current local WAL baseline.

Failure of a gate reopens this ADR. It does not authorize silently filling a
Ratis gap with a second authority mechanism.

## Decisions deliberately deferred

- exact partition-count defaults and maximums;
- exact stable hash algorithm and seed format;
- automatic partition-count increase or repartitioning;
- shared per-volume journal and cross-partition group commit;
- zone-aware placement policy when topology labels are incomplete;
- learner replacement and joint-consensus workflow details;
- cross-region replication;
- exactly-once delivery or transaction support;
- storage-format migration from the learning-stage local engine.

## References

- [Partitioned Multi-Raft design](../design/v0.30-partitioned-multiraft-design.md)
- [Partition model](../architecture/appendix-a-partition-model.md)
- [Replication protocol](../architecture/appendix-b-replication-protocol.md)
- [Storage architecture](../design/storage-architecture.md)
- [Apache Ratis](https://ratis.apache.org/)
- [Apache BookKeeper](https://bookkeeper.apache.org/docs/overview/)
