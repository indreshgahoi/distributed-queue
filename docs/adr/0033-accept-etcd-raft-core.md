# ADR 0033: Accept etcd/raft as the Consensus Core and Own the Multi-Raft Host

## Status

Accepted for G1.1.

This accepts `go.etcd.io/raft/v3 v3.7.0` as the target consensus algorithm
dependency. It does not claim that the current queue node has a production
commit authority. The durable log, transport, snapshot transfer, proposal
completion, scheduling, and group lifecycle still have to be implemented and
qualified.

## Context

ADR 0031 deferred Dragonboat v4 because the upstream project still describes
that API line as unstable development. That remains true at this review. The
project cannot make an unreleased branch the durable authority for customer
messages merely because the experimental G2 adapter works.

The alternative is to adopt a stable consensus core and make the previously
hidden host-runtime work an explicit part of this queue's architecture.
`etcd/raft` is actively released, has broad production lineage, exposes a
deterministic `RawNode`, and leaves disk and network I/O to its caller.

The exact accepted version is:

```text
module: go.etcd.io/raft/v3
version: v3.7.0
minimum Go version declared upstream: 1.26
project toolchain: go1.27.1
```

## Decision

Use etcd/raft as the only target data-plane consensus core.

The project owns a shared Multi-Raft host around it:

```text
Queue command
  -> proposal tracker
  -> partition RawNode
  -> Ready dispatcher
       -> durable per-volume Raft storage
       -> shared peer transport
       -> committed-entry apply scheduler
  -> deterministic queue state machine
  -> complete the client request after commit and apply
```

The host must preserve the synchronous `Ready` contract in this order:

1. persist snapshot, hard state, and new entries;
2. honor `MustSync` before releasing dependent messages;
3. send outbound Raft messages;
4. apply only committed entries, in order;
5. advance the `RawNode` only after that work is accepted;
6. complete a queue proposal only after its command result is applied.

Calling `RawNode.Propose` successfully is not a client acknowledgement. The
G1.1 proof demonstrates that proposal submission can succeed while two of
three replicas are unavailable and no entry commits.

### Storage direction

Start with a project-owned, per-volume multiplexed Raft WAL and durable hard
state store. One physical append batch may contain entries for several groups,
but completion is released only after the batch sync succeeds. Per-group term
and index identity remains explicit. A storage failure poisons the affected
volume runtime and cannot be reported as success.

Do not add a second application WAL. The durable authority is:

```text
valid Raft snapshot
  + durable Raft hard state
  + checksum-valid committed log suffix
  = recoverable partition replica
```

### Transport and scheduling direction

Use shared, bounded peer connections and batch messages by destination node.
Use a sharded tick/Ready scheduler rather than one goroutine and ticker per
group. Fairness and admission are enforced per tenant, group, peer, and volume.

### Migration of the G2 evidence

The Dragonboat G2 module remains historical executable evidence for queue
semantics and failure cases. It is not promoted. Its tests become acceptance
cases for the etcd/raft host as each owned layer becomes real.

## Evidence collected in G1.1

The isolated proof establishes:

- leader election and committed convergence across three replicas;
- fail-closed application without a quorum;
- the distinction between proposal acceptance and committed application;
- snapshot plus retained-state restart and continued commits;
- executable ordering of persistence, messaging, apply, and advance;
- a lower-bound 1,000-core construction baseline.

The proof uses memory storage. It does not qualify power-loss durability,
snapshot transfer, network behavior, or complete-host density. Those are exit
criteria for the owned runtime, not properties supplied by the consensus core.

## Why this is preferable to waiting for Dragonboat v4

Waiting leaves the project architecture dependent on an upstream release with
no committed date. Accepting etcd/raft removes that external block and makes
the engineering cost visible. The work is substantial, but it is directly
relevant to this project's goals: multiplexed durability, bounded scheduling,
failure isolation, recovery, and multi-tenant fairness.

The project is not implementing Raft. It is integrating a maintained Raft core
into a storage and messaging product.

## Consequences

### Positive

- the consensus algorithm has a supported, exact release;
- the domain remains vendor-independent behind `consensus.Group`;
- storage, batching, volume binding, and tenant fairness are under project
  control and can be measured directly;
- the architecture no longer waits on Dragonboat v4 release status;
- deterministic core tests can inject messages and storage failures precisely.

### Negative

- the project now owns correctness-sensitive Ready processing;
- transport, durable storage, snapshot transfer, and Multi-Raft scheduling are
  significant product code;
- the experimental Dragonboat adapter is not the production implementation;
- production promotion requires more evidence than the G1.1 core proof.

## Rejected alternatives

### Approve Dragonboat v4 development head

Rejected for the same supportability reason recorded in ADR 0031.

### Adopt Dragonboat v3

Rejected. It is the older stable API line, while the target was designed around
v4 behavior; adopting it would exchange one release risk for a legacy API and
still require a fresh compatibility and maintenance review.

### Use HashiCorp Raft per partition

Rejected for the target path. Its independently hosted group shape does not
address shared scheduling, connection, WAL, and file-descriptor density. It
would require another density proof before becoming a better fit.

### Put message consensus in the control-plane etcd cluster

Rejected. A global external quorum would couple customer-message availability
and throughput to control-plane operations and add an extra authority hop.

### Implement the Raft algorithm

Rejected. The project owns the host around a maintained core, not election or
log-matching algorithm correctness.

## Next implementation slice

Build one durable single-group host around `RawNode` with:

- file-backed hard state and entries;
- checksum and torn-tail recovery;
- the exact Ready ordering above;
- proposal correlation through committed apply;
- restart, disk-failure, and ambiguous-client-result tests.

Only after that slice is correct should the host multiplex groups, connections,
ticks, and fsync across a node.

## References

- [ADR 0030](0030-go-target-runtime-and-consensus-gate.md)
- [ADR 0031](0031-defer-dragonboat-v4.md)
- [G1.1 executable proof](../../tools/etcd-raft-proof/README.md)
- [G1.1 benchmark](../benchmarks/g1.1-etcd-raft-core/README.md)
- [etcd/raft repository](https://github.com/etcd-io/raft)
- [etcd/raft v3.7.0 release](https://github.com/etcd-io/raft/releases/tag/v3.7.0)
- [etcd/raft usage contract](https://github.com/etcd-io/raft/blob/v3.7.0/README.md)
