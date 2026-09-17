# ADR 0031: Defer Dragonboat v4 as a Production Dependency

## Status

Accepted as the G1 gate decision: **defer Dragonboat v4**.

This does not reject Dragonboat permanently. It rejects adding the evaluated
development revision to the production module now.

## Context

ADR 0030 requires evidence before a consensus library becomes message commit
authority. The target needs a complete Multi-Raft host, not only a Raft state
machine. Dragonboat is a strong architectural match, so G1 evaluated its v4
API in an isolated proof module.

The evaluated revision is:

```text
module: github.com/lni/dragonboat/v4
version: v4.0.0-20250723143628-076c7f6497dc
commit:  076c7f6497dcc18880aed6323246d5079661942c
```

At evaluation time, the upstream project still describes v3.3 as the stable
release line and the v4 branch as unstable development. The target design needs
v4, so using this revision would make a moving, unreleased branch the durable
authority for customer messages.

## Evidence

The isolated executable proof demonstrates:

- one three-replica group elects a leader;
- `SyncPropose` returns the deterministic state-machine apply result;
- all available followers eventually converge on the committed value;
- loss of two replicas prevents a successful proposal;
- a snapshot plus retained Raft state restores the applied value after all
  local `NodeHost` state is closed and reopened;
- Dragonboat types remain outside the production domain and root Go module.

The local proposal benchmark and its limitations are recorded under
`docs/benchmarks/g1-dragonboat-v4/`.

The proof does **not** establish:

- power-loss or torn-write durability at the exact client-success boundary;
- snapshot transfer and interrupted installation behavior;
- per-group placement across twenty or more stable volumes;
- recovery and resource behavior at 1,000 or 10,000 groups;
- disk and network amplification under realistic payloads;
- a supported upgrade path between deployed v4 revisions.

These are unresolved, not silently passed.

## Decision

Do not add Dragonboat v4 to the root production module and do not begin G2 on
this revision.

Keep the proof in the nested `tools/consensus-proof` module as reproducible
evidence. The nested module prevents an experimental dependency from entering
production binaries or their dependency graph.

G1 is complete as a decision gate, with outcome **defer**. The next architecture
decision must choose one of these paths:

1. re-run this gate when Dragonboat publishes a supported v4 release and an
   upgrade policy is clear; or
2. evaluate an alternative using the same owned `consensus.Group` contract,
   including the full engineering cost of storage, transport, snapshots, and
   Multi-Raft scheduling when considering a consensus-core-only library.

No candidate may advance merely because it can run a three-node demo.

## Why the remaining benchmarks stop here

Release support is a mandatory gate, not a weighted preference. Group-density
and storage benchmarks can compare implementations, but they cannot turn an
explicitly unstable development line into a supportable durability dependency.
Stopping the approval sequence after collecting enough functional and baseline
evidence avoids both false confidence and throwaway optimization work.

The deferred measurements remain mandatory before any later approval.

## Consequences

### Positive

- the project does not disguise development code as a production dependency;
- the queue domain stays independent of a consensus vendor;
- the proof and benchmark are repeatable when a stable candidate appears;
- G2 cannot accidentally claim majority durability on incomplete evidence.

### Negative

- the first replicated Go partition remains blocked;
- Dragonboat's density and volume behavior remains unknown for this project;
- another candidate may require substantially more host-runtime engineering.

## Rejected alternatives

### Approve the pinned v4 commit because the proof passes

Rejected. Functional behavior at one revision does not provide maintenance,
security-response, or safe-upgrade support for a durable authority.

### Fall back to stable Dragonboat v3

Rejected without a separate compatibility review. The target design was based
on v4 APIs; silently redesigning around v3 would bypass the gate.

### Build custom Raft immediately

Rejected. The project should demonstrate a reliable queue, not create a new
consensus implementation without evidence that available libraries fail the
requirements.

## References

- [ADR 0030](0030-go-target-runtime-and-consensus-gate.md)
- [Executable proof](../../tools/consensus-proof/README.md)
- [Benchmark evidence](../benchmarks/g1-dragonboat-v4/README.md)
- [Dragonboat repository](https://github.com/lni/dragonboat)
- [Dragonboat releases](https://github.com/lni/dragonboat/releases)
- [Dragonboat storage documentation](https://github.com/lni/dragonboat/blob/master/docs/storage.md)
