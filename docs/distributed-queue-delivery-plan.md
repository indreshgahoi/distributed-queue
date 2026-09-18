# Distributed Queue Delivery Plan

This plan converts the target architecture into reviewable milestones. Each
phase follows: semantics, invariants, failure scenarios, tests, implementation,
regression tests, documentation, commit, and release.

## Current Go target-runtime plan

[ADR 0030](adr/0030-go-target-runtime-and-consensus-gate.md) supersedes the
uncompleted Java/Ratis direction below. The older numbered phases remain in
this document as history for the Java baseline; they are not the active order
for new distributed-runtime work.

### G0 — Authority and deterministic-domain foundation

Status: implemented; distributed data-plane claims remain explicitly disabled.

- Go module and project-owned control/data-plane boundaries;
- deterministic partition commands and snapshots;
- signed partition-aware receipts and stable routing;
- atomic PostgreSQL catalog, idempotency record, and append-only outbox;
- PostgreSQL logical replication to monotonic etcd projections without polling;
- deployable metadata API and coordination projector;
- real PostgreSQL/etcd integration tests and local Compose stack.

Exit evidence: Go tests and vet pass, adapter integration tests pass against
real dependencies, container images build, stack smoke test observes the exact
PostgreSQL queue generation in etcd, and the full Java regression remains green.

### G1 — Multi-Raft dependency approval gate

Status: complete as a gate decision; Dragonboat v4 is deferred, not approved.

Limitation solved: the project has a deterministic state machine but no
supported replicated commit authority.

- pin and record an exact candidate version or commit;
- prove proposal completion means majority-durable commit plus apply;
- prove restart, log repair, snapshot, compaction, and installation behavior;
- measure idle and active group density, startup recovery, file descriptors,
  memory, disk amplification, and publish percentiles;
- prove stable volume binding and clean containment behind `consensus.Group`;
- decide accept, fork, choose another library, or defer with evidence.

No production Dragonboat dependency is accepted while only the old v3 release
line is stable and the required v4 API remains a moving development branch.

Evidence: the pinned isolated proof passed leader/apply, quorum-loss, and local
snapshot/restart tests and produced a proposal baseline. The mandatory
supportability gate failed, so high-density, multi-volume, and fault-injection
approval work remains intentionally unresolved. See
[ADR 0031](adr/0031-defer-dragonboat-v4.md) and the
[G1 evidence](benchmarks/g1-dragonboat-v4/README.md).

### G1.1 — Resolve the consensus implementation

Status: complete as an architecture and dependency decision. ADR 0033 accepts
`go.etcd.io/raft/v3 v3.7.0` as the consensus core and makes the durable
Multi-Raft host a project-owned subsystem.

Limitation solved: G1 proved the architecture boundary but found no approved
production consensus dependency, so G2 still has no supported commit authority.

- Dragonboat v4 remains deferred while upstream labels it unstable;
- the accepted etcd/raft core is pinned and exercised in an isolated proof;
- persistence-before-send, committed-only apply, quorum loss, and restart
  behavior are executable;
- the storage, transport, snapshot, proposal-tracking, and scheduler work is
  explicitly owned rather than attributed to the library;
- complete-host density, multi-volume, power-loss, snapshot-install, and
  sustained-workload evidence remains mandatory before production promotion.

Evidence: [ADR 0033](adr/0033-accept-etcd-raft-core.md), the
[executable proof](../tools/etcd-raft-proof/README.md), and the
[G1.1 baseline](benchmarks/g1.1-etcd-raft-core/README.md).

### G2.1 — Durable single-group etcd/raft host

Status: next implementation slice.

Limitation solved: accepting a consensus algorithm does not provide a durable
queue replica. The host must correctly join disk, network, apply, and client
completion around `RawNode`.

- persist hard state and log entries before dependent Raft messages;
- implement checksummed frames and torn-tail recovery;
- honor `Ready.MustSync` and poison the writer after uncertain append failure;
- correlate a command ID through committed state-machine apply;
- recover snapshot plus retained log without exposing an uncommitted suffix;
- test restart, disk full, partial append, quorum loss, and ambiguous client
  response boundaries;
- benchmark force-per-entry before selecting group-commit policy.

Exit evidence: one three-replica queue partition uses the supported core and
project-owned durable host; acknowledged commands survive one replica failure
and restart under the documented filesystem assumptions.

### G2 — One real replicated partition

Status: implemented as isolated Dragonboat evidence. It supplies semantic and
failure acceptance cases for G2.1, but is not the production implementation.

Limitation solved: current Go operations cross only a local test adapter.

- run one partition on three queue-node processes;
- commit publish, claim, ACK, NACK, expiry, and DLQ transitions through Raft;
- acknowledge only the consensus adapter's documented durable completion;
- restore from Raft snapshot plus retained log;
- verify leader crash before and after client acknowledgement.

Evidence: the adapter and runnable three-node process implement the complete
queue lifecycle through a real Raft group. Automated tests cover convergence,
quorum loss, idempotent retry after an ambiguous response, and snapshot plus
log restart. A container smoke test covers leader election and failover. See
[ADR 0032](adr/0032-experimental-g2-replicated-partition.md), the
[runbook](runbooks/g2-experimental-cluster.md), and the
[benchmark](benchmarks/g2-replicated-partition/README.md).

### G3 — Multi-group node runtime and control-plane watches

Limitation solved: one replicated group does not support many tenant queues.

- register fenced node incarnations and volume inventory in etcd;
- consume desired placement with list-at-revision plus watch-from-next-revision;
- host many groups through shared transport, storage, and schedulers;
- preserve stable replica-to-volume binding across restart and config reorder;
- add per-tenant and per-volume admission/fairness limits.

### G4 — Gateway and multi-partition queue

Limitation solved: customers cannot use a stable endpoint or scale a queue
beyond one partition.

- publish versioned routes through etcd;
- route keyed and unkeyed publish deterministically;
- use bounded, fair receive probes rather than full partition fan-out;
- route ACK/NACK directly from signed receipt lineage;
- retry only outcomes proven safe by command idempotency.

### G5 — Repair, membership change, and operational hardening

Limitation solved: the system cannot yet replace failed replicas or demonstrate
bounded recovery under correlated failures.

- snapshot transfer and learner catch-up;
- safe membership change and promotion eligibility;
- node drain, disk drain, and permanent replica replacement;
- network partition, disk-full, corruption, slow-follower, and repeated-crash
  fault injection;
- SLO dashboards, retained-WAL safety limits, backup/restore, and runbooks.

Each phase has a benchmark gate when it changes fsync, network quorum, snapshot
I/O, group density, recovery load, hot-path routing, or fairness.

## Historical Java delivery plan

## Phase 0 — Close v0.27 Honestly

Goal: finish bounded transport without implying automatic replication.

- verify HTTP client timeout and batch-bound tests;
- test partial-prefix failure and unchanged retry;
- document that replica membership and scheduling do not exist;
- do not call follower data committed.

## Phase 1 — v0.27.1: Replication Performance Baseline

Status: complete for the architecture available at v0.27. Raw results and
explicit dependency-bound matrix entries are recorded under
`benchmarks/v0.27.1/`; snapshot-install and representative end-to-end network
profiles become required when those protocols exist.

Limitation solved: the design does not know whether record-by-record force,
encoding, locks, HTTP, snapshot I/O, or partition density is the actual
bottleneck.

- benchmark forced leader WAL append by payload and concurrency;
- benchmark the current follower batch, which forces every record;
- prototype batch write plus one force without changing production semantics;
- measure batch sizes 1, 8, 32, 128, and 256;
- measure segment rotation and concurrent snapshot interference;
- measure HTTP serialization separately from follower disk durability;
- measure many idle and active partitions on one node;
- check in raw JMH JSON, environment metadata, and interpretation.

Exit criterion: v0.28 storage APIs are selected from measured evidence, with
p50/p95/p99/p99.9/max and throughput recorded. Full matrix:
[Group Commit and Durability](architecture/appendix-c-group-commit-and-durability.md).

## Phase 2 — v0.28: Durable Logical Replicated Log

Limitation solved: record counting cannot preserve logical sequence across WAL
reclamation and does not store the leader term with each entry.

- encode `logIndex` and `logTerm` atomically with each replicated WAL entry;
- expose bounded reads by logical index;
- persist replica hard state: current term, vote, and commit index;
- recover `lastApplied` from the durable snapshot plus committed replay rather
  than persisting a marker that could outlive its in-memory state;
- extend snapshots with last included index and term;
- define upgrade policy explicitly; no migration is required for this learning
  repository unless chosen before implementation;
- prohibit compaction from destroying required logical history.

Exit criterion: restart and snapshot plus WAL recovery reproduce
`lastApplied <= commitIndex <= lastLogIndex` without counting reclaimed records.

## Phase 3 — v0.29: Replica Membership and Placement

Status: implemented and verified.

Limitation solved: the system cannot identify which nodes should store a
partition, so v0.27 cannot schedule replication safely.

Design review:
[problem and proposed solution](design/v0.29-replica-membership-problem-and-solution.md),
[HLD](design/v0.29-replica-membership-hld.md),
[LLD](design/v0.29-replica-membership-lld.md),
[semantics](design/v0.29-replica-membership-semantics.md),
[failure scenarios](design/v0.29-replica-membership-failure-scenarios.md), and
[ADR 0028](adr/0028-immutable-initial-replica-membership.md).

- add queue-generation replication factor, default three;
- model voter versus learner replicas;
- place replicas across distinct live nodes and, later, failure domains;
- prevent one node from hosting two replicas of the same partition;
- expose desired membership to queue nodes;
- keep membership immutable after bootstrap in this phase.

Exit criterion: every partition has one inspectable desired replica set and
nodes materialize only assigned replicas.

## Phase 4 — v0.30: Automatic Catch-Up and Learner Bootstrap

Limitation solved: follower transport exists but no controller drives it.

- start bounded per-follower replication workers on the current leader;
- maintain `nextIndex` and `matchIndex` per follower;
- retry with bounded exponential backoff and jitter;
- expose lag, last success, and failure metrics;
- install a snapshot when requested history has been reclaimed;
- rate-limit recovery across many partitions on one node.

Exit criterion: an assigned learner automatically reaches the leader's log end
without blocking local queue mutation locks.

Benchmark gate: demonstrate bounded threads/connections, fair progress across
partitions, backoff under unavailable followers, and controlled memory/WAL growth
under a slow follower.

## Phase 5 — v0.31: Majority Commit and Committed-Only Apply

Limitation solved: a local append is acknowledged even though loss of that node
can lose the operation.

- compute majority from voting membership;
- advance monotonic `commitIndex` from follower match indexes;
- propagate `leaderCommit` in append and heartbeat requests;
- apply queue transitions only through committed index;
- acknowledge publish, lease, ACK, NACK, expiry, and DLQ transitions only after
  majority commit;
- stop mutations immediately after losing majority authority.

Exit criterion: acknowledged mutations survive any one replica failure with
replication factor three.

Benchmark gate: decompose end-to-end p99 into leader queueing, leader force,
network, follower force, commit propagation, and state-machine apply.

## Phase 6 — v0.32: Node-Coordinated Leader Election

Limitation solved: leader failure still requires external/manual authority.

- implement follower, candidate, and leader roles per partition;
- add randomized election timeouts and heartbeats;
- durably persist term and vote before responding;
- require majority vote and Raft-style log freshness;
- fence old leaders using terms;
- report elected leadership to metadata for discovery;
- keep PostgreSQL outside the voting and message commit path.

Exit criterion: a three-replica partition elects a safe leader after one node
failure and refuses two simultaneous majority-capable leaders.

## Phase 7 — v0.33: Safe Promotion and Replica Repair

Limitation solved: returning nodes and divergent uncommitted suffixes cannot yet
be reconciled automatically.

- truncate only uncommitted conflicting suffixes under current leader authority;
- require committed state application before serving traffic;
- promote caught-up learners through a safe membership transition;
- replace failed replicas without reducing the old majority prematurely;
- test partitions, repeated crashes, and recovery during snapshot transfer.

Exit criterion: permanent node loss is repaired while preserving every
majority-acknowledged transition.

Benchmark gate: measure foreground p99 while installing snapshots and while
recovering many replicas after one node failure.

## Phase 8 — Multi-Partition Queue Semantics

Limitation solved: each queue still has only partition zero.

- make partition count immutable per generation;
- add stable keyed publication routing;
- add bounded rotating receive probes;
- carry authenticated partition identity in receipt handles;
- route ACK and NACK to the current leader of the originating partition;
- document partition-local ordering and approximate empty receive.

Exit criterion: one customer queue scales across partitions without claiming a
global order.

Benchmark gate: measure keyed distribution, hot partitions, bounded receive
probe hit rate, and node resource use as partition count increases.

## Issue-Ready Backlog

Create one GitHub milestone per phase and one issue per bullet group below.

### v0.27.1

1. Capture forced-WAL baseline by payload and concurrency.
2. Benchmark current per-record-force follower batches.
3. Prototype and benchmark one-force batch append.
4. Measure segment rotation and snapshot interference.
5. Measure HTTP-only and HTTP-plus-disk follower paths.
6. Measure idle and active partition density.
7. Check in raw results, environment manifest, and engineering interpretation.

### v0.28

1. Specify replicated log frame and hard-state formats.
2. Implement logical-index append and bounded read contracts.
3. Persist and recover term, vote, commit index, and applied index.
4. Extend snapshot authority with last included index and term.
5. Integrate replication-safe WAL reclamation.
6. Add crash matrix and property/state-machine tests.

### v0.29

1. Add replication factor to queue-generation metadata.
2. Model voter and learner replica membership.
3. Implement distinct-node, least-loaded initial replica placement.
4. Add per-member fenced provisioning and activation after all members are ready.
5. Expose membership inspection APIs and operational logs.

### v0.30

1. Implement per-follower next-index and match-index tracking.
2. Add bounded replication scheduler with backoff and jitter.
3. Implement snapshot installation protocol.
4. Add recovery bandwidth and concurrency limits.
5. Add replica lag metrics and fault-injection integration tests.

### v0.31

1. Define majority commit semantics for every queue transition.
2. Implement leader commit-index advancement.
3. Propagate leader commit and implement follower committed-only apply.
4. Gate client responses on majority commit.
5. Test leader crash before and after quorum acknowledgement.

### v0.32

1. Specify election timers, terms, votes, and role transitions.
2. Implement durable vote granting and log freshness checks.
3. Implement heartbeats and majority-loss step-down.
4. Integrate gateway discovery with reported leadership.
5. Build deterministic partition and split-vote tests.

### v0.33

1. Implement conflict discovery and uncommitted suffix truncation.
2. Implement learner promotion and safe membership transition.
3. Implement dead-replica replacement workflow.
4. Test returning stale nodes and repeated failures during repair.

### Multi-partition

1. Define immutable queue-generation partition configuration.
2. Implement stable keyed publish routing.
3. Implement bounded fair receive probing.
4. Add signed partition-aware receipt handles.
5. Add partition-local ordering and routing failure tests.

## GitHub Publishing

Remote issue creation requires an authenticated GitHub CLI session:

```bash
gh auth login -h github.com
```

After authentication, publish milestones and issues from this plan. Keep issue
titles problem-oriented and include invariant, failure tests, non-goals, and exit
criteria in every issue body.
