# ADR 0030: Build the Target Runtime in Go and Gate the Consensus Dependency

## Status

Accepted for the target architecture.

Amended by [ADR 0033](0033-accept-etcd-raft-core.md), which completes the gate
by accepting `go.etcd.io/raft/v3 v3.7.0` and assigning the surrounding durable
Multi-Raft host to this project. Candidate-specific Dragonboat statements below
record the context at the time of this decision.

This ADR supersedes ADR 0001's language choice and ADR 0029's Apache Ratis
preference for new target-runtime work. Those ADRs remain historical records of
the Java learning path; they are not instructions to delete the Java baseline.

## Context

The Java implementation taught and validates local queue durability, segmented
WAL recovery, snapshots, lifecycle fencing, routing, and early follower
replication. The next system must host many independently replicated partition
groups per process, use shared storage and transport resources, and remove
PostgreSQL from the message commit path.

Continuing to evolve both the Java runtime and a new distributed runtime would
split architecture ownership. Deleting Java immediately would also remove the
only mature semantic oracle before the replacement has parity.

The control-plane RFC selects PostgreSQL as durable metadata authority, an
append-only transactional outbox, logical replication rather than polling, and
etcd as rebuildable coordination state. The data-plane RFC selects one Raft
group per queue partition and a project-owned consensus port.

Dragonboat is the leading Go multi-Raft candidate. At this review, its official
repository identifies v3.3.x as the released stable line and the v4 module as an
unstable development branch. The target design depends on v4-style APIs and
cannot honestly claim a supported production dependency until that mismatch is
resolved by an explicit review.

## Decision

All new target-runtime components are implemented in Go.

The Java system remains temporarily as:

- the executable semantic oracle for queue behavior;
- a source of recovery and durability failure tests;
- benchmark history for comparing the new runtime;
- a rollback reference while Go behavior is incomplete.

It receives no new distributed architecture unless required to repair the
baseline. Removal requires a separate migration decision after semantic,
recovery, and benchmark parity is demonstrated.

The target control plane uses:

```text
Go metadata API
  -> PostgreSQL aggregate + idempotency record + append-only outbox
  -> pgoutput logical replication
  -> Go projector
  -> version-monotonic etcd desired state
```

The target data plane uses one project-owned `consensus.Group` boundary per
partition. Domain packages must not import the selected Raft library.

Dragonboat remains a candidate, not an accepted dependency. Integration is
allowed only after a proof records:

1. a supportable version and upgrade policy;
2. majority-durable proposal completion semantics;
3. snapshot save, restore, transfer, and compaction behavior;
4. restart and torn-process recovery behavior;
5. stable multi-volume directory binding;
6. group-density memory, file-descriptor, startup, and idle-CPU measurements;
7. sustained and burst publish latency under fsync and group commit;
8. clean containment behind the project-owned consensus port.

The first proof uses the library's default LogDB. A custom multiplexed WAL is
not built unless benchmark evidence identifies a material limitation that the
default storage cannot address.

The first evaluation is complete. ADR 0031 defers the evaluated Dragonboat v4
development revision after the mandatory supportability gate failed. Its
isolated functional proof remains evidence; it is not a production dependency.

## Consequences

### Positive

- The target has one implementation language and one set of runtime boundaries.
- Consensus machinery is not reimplemented as queue business logic.
- PostgreSQL is removed from provisioning polling and from message commits.
- etcd can be rebuilt because it is a projection, not durable catalog authority.
- The consensus library can be replaced without changing queue-domain types.
- Java removal becomes an evidence-based migration rather than a destructive
  rewrite step.

### Negative

- Two language trees coexist during migration.
- Cross-language semantic tests and benchmark comparisons are required.
- Distributed data-plane delivery waits for the consensus dependency gate.
- The Go control plane is useful before the Go data plane is complete, so the
  repository must label current and target guarantees precisely.

## Rejected alternatives

### Delete Java immediately

Rejected until the Go runtime has semantic, recovery, and performance parity.
Early deletion would reduce evidence and increase rewrite risk without
improving the target architecture.

### Continue with Apache Ratis

Rejected for new work because it would keep the target data plane in Java after
the language decision and require maintaining two new-runtime directions.

### Pin Dragonboat v4 master without a gate

Rejected. A moving development branch is not an acceptable invisible
durability dependency. Any temporary proof-of-concept pin must record its exact
commit and cannot be described as a supported release.

### Build custom Raft

Rejected as the production direction. It would turn the project into a
consensus-library effort instead of demonstrating queue semantics,
multi-tenancy, storage, operations, and failure engineering.

## References

- [Control-plane architecture RFC](../architecture/Control_Plane_Architecture_RFC.md)
- [Data-plane architecture RFC](../design/Data_Plane_Architecture_RFC.md)
- [ADR 0031: defer Dragonboat v4](0031-defer-dragonboat-v4.md)
- [G1 proof results](../benchmarks/g1-dragonboat-v4/README.md)
- [Dragonboat repository](https://github.com/lni/dragonboat)
- [Dragonboat storage documentation](https://github.com/lni/dragonboat/blob/master/docs/storage.md)
