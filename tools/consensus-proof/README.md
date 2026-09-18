# Dragonboat v4 Dependency Proof

This isolated Go module contains the executable evidence for the G1
consensus-library gate and the experimental G2 replicated queue partition. It
is not production queue-node code and its Dragonboat dependency must not be
copied into the root module while ADR 0031 remains deferred.

The proof pins this exact module version:

```text
github.com/lni/dragonboat/v4
v4.0.0-20250723143628-076c7f6497dc
```

It exercises a real three-replica group over localhost and verifies:

- a synchronous proposal returns the state-machine apply result;
- followers eventually apply the committed proposal;
- a proposal cannot succeed after quorum is removed;
- snapshot creation followed by full process-level host close and restart
  reconstructs the applied state.

The `proof` package deliberately uses a tiny counter state machine. The
`queuegroup` package then adapts the real deterministic queue domain through
the project-owned `consensus.Group` interface without leaking Dragonboat types
into the domain.

## Experimental G2 cluster

The module also provides:

- a Dragonboat-backed `consensus.Group` adapter;
- three-replica queue lifecycle and recovery tests;
- leader-loss and ambiguous-response retry tests;
- a runnable queue-node HTTP process;
- a three-node `compose.g2.yaml` topology.

See the [G2 runbook](../../docs/runbooks/g2-experimental-cluster.md). This is
integration evidence, not a reversal of ADR 0031.

## Run

The tests bind temporary localhost ports.

```bash
cd tools/consensus-proof
go test ./...
go test ./proof \
  -run '^$' \
  -bench '^BenchmarkThreeReplicaSyncProposal$' \
  -benchtime=100x \
  -count=3
```

The benchmark disables automatic snapshots so that it measures only the
replicated proposal path. It is a dependency comparison baseline, not a
production throughput claim.

## Deliberate limits

This module does not establish power-loss durability, torn-write repair,
snapshot transfer, stable multi-volume group placement, or high group density.
Those expensive gates were not treated as passed after the candidate failed
the earlier supportability gate. See
[ADR 0031](../../docs/adr/0031-defer-dragonboat-v4.md) and the checked-in
[benchmark record](../../docs/benchmarks/g1-dragonboat-v4/README.md).
