# Distributed Queue Architecture Handbook

This directory is the living design reference for the distributed queue. Read
the documents in order when learning the system or reviewing a milestone.

1. [Partition model](appendix-a-partition-model.md) — how tenants, queues,
   partitions, replicas, and nodes relate.
2. [Replication protocol](appendix-b-replication-protocol.md) — leader/follower
   flow, ordering, catch-up, commit, and failure behavior.
3. [Group commit and durability](appendix-c-group-commit-and-durability.md) —
   how batching reduces force cost without weakening acknowledgement semantics.
4. [Metadata model](appendix-d-metadata-model.md) — proposed control-plane
   tables, ownership, constraints, and what must not live in PostgreSQL.
5. [Guarantee matrix](appendix-e-guarantee-matrix.md) — guarantees by operation,
   failure, and implementation phase.

The concise target is in
[Distributed Queue Target Architecture](../distributed-queue-target-architecture.md),
and implementation order is in the
[Distributed Queue Delivery Plan](../distributed-queue-delivery-plan.md).

Milestone-specific designs:

- [Go control-plane architecture RFC](Control_Plane_Architecture_RFC.md) —
  PostgreSQL authority, logical-replication outbox, etcd coordination, and
  failure boundaries.
- [Go data-plane architecture RFC](../design/Data_Plane_Architecture_RFC.md) —
  deterministic state machine, multi-Raft boundary, storage, routing, and
  target guarantees.
- [ADR 0030: Go target runtime and consensus gate](../adr/0030-go-target-runtime-and-consensus-gate.md)
  — language migration, Java retention, and the Dragonboat approval boundary.
- [ADR 0031: defer Dragonboat v4](../adr/0031-defer-dragonboat-v4.md) — G1
  executable evidence, failed supportability gate, and the next decision point.
- [G1 Dragonboat proof results](../benchmarks/g1-dragonboat-v4/README.md) —
  reproducible functional and proposal-baseline evidence with explicit limits.
- [Storage architecture](../design/storage-architecture.md) — current files,
  recovery authority, and phased evolution to replicated storage.
- [v0.28 high-level design](../design/v0.28-durable-log-hld.md)
- [v0.28 low-level design](../design/v0.28-durable-log-lld.md)
- [ADR 0027: durable logical replicated log](../adr/0027-durable-logical-replicated-log.md)
- [v0.29 problem and proposed solution](../design/v0.29-replica-membership-problem-and-solution.md)
- [v0.29 high-level design](../design/v0.29-replica-membership-hld.md)
- [v0.29 low-level design](../design/v0.29-replica-membership-lld.md)
- [v0.29 semantics](../design/v0.29-replica-membership-semantics.md)
- [v0.29 failure scenarios](../design/v0.29-replica-membership-failure-scenarios.md)
- [ADR 0028: immutable initial replica membership](../adr/0028-immutable-initial-replica-membership.md)

## Living-document rules

- Update these documents before implementing a changed invariant.
- Label statements as **current**, **target**, or **deferred**.
- Link accepted choices to an ADR once implementation begins.
- Keep physical storage facts separate from control-plane observations.
- Record benchmark environment and raw results; do not publish only averages.
- A diagram is explanatory, not authoritative when it conflicts with semantics
  or executable tests.
