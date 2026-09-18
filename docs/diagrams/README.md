# Architecture Diagrams

These diagrams explain cross-component protocols whose correctness depends on
ordering, authority, or failure handling.

For the complete learning-oriented design, start with the
[architecture handbook](../architecture/README.md).

- [Distributed target architecture](distributed-target-architecture.md) —
  target control/data-plane boundary, multi-tenant node layout, replica state,
  and partition-level node-failure recovery.
- [G2 provisioning and replication](g2-provisioning-and-replication.md) — code
  map, Raft publish/receive flows, current manual boundary, and the target
  automatic queue-provisioning sequence.

- [Provisioning claim sequence](provisioning-claim-sequence.md) — interactions
  between the queue node, metadata service, PostgreSQL, and local WAL storage.
- [Provisioning claim decision flow](provisioning-claim-flow.md) — decisions in
  one reconciliation cycle, including failures, lease expiry, and takeover.
- [Node registration and placement flow](node-registration-placement-flow.md) —
  process-incarnation fencing, heartbeat renewal, initial placement, and
  placement-aware provisioning.
- [Runtime partition lifecycle](runtime-partition-lifecycle.md) — durable queue
  progression, fenced recovery, readiness publication, and deactivation.
- [Data-plane operation lifecycle](data-plane-operation-lifecycle.md) — guarded
  request admission, WAL delegation, and deterministic closure race ordering.
- [Follower WAL append flow](follower-wal-append-flow.md) — lineage, sequence,
  retry, conflict, and durable leader-epoch fencing decisions.
- [Replica catch-up sequence](replica-catch-up-sequence.md) — bounded leader
  reads, node-to-node transport, forced follower progress, and retry behavior.
