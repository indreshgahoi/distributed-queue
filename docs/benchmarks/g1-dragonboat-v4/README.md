# G1 Dragonboat v4 Proof Results

## Decision context

This is a narrow dependency-gate baseline. It measures a real three-replica
Dragonboat group on one development machine. It does not estimate production
capacity and does not approve the dependency. The decision is recorded in
[ADR 0031](../../adr/0031-defer-dragonboat-v4.md).

## Candidate

| Field | Value |
|---|---|
| Module | `github.com/lni/dragonboat/v4` |
| Version | `v4.0.0-20250723143628-076c7f6497dc` |
| Commit | `076c7f6497dcc18880aed6323246d5079661942c` |
| LogDB | Dragonboat default Pebble LogDB, tiny-memory configuration |
| Replicas | 3 `NodeHost` instances over localhost |
| State machine | 16-byte deterministic counter snapshot |
| Snapshot policy in proposal benchmark | disabled |

The tiny-memory LogDB profile is intentional. The default expert profile
advertises an 8 GiB LogDB memory limit, which is inappropriate for a small
dependency proof; the library's tiny profile advertises 256 MiB. This is a
configuration observation, not a measured resident-memory result.

## Environment

```text
Date:       2026-09-17
OS:         Linux 5.15.0-191-generic x86_64
CPU:        Intel Core i7-8700 @ 3.20 GHz, 6 cores / 12 threads
Go:         go1.27.1 linux/amd64
Repository: 6378ec8 plus the uncommitted G1 proof
Filesystem: temporary directories on the local development filesystem
Network:    localhost TCP
```

## Functional results

```text
TestThreeReplicaProposalCompletesAfterApply  PASS
TestSnapshotAndRestartRecoverAppliedState    PASS
TestProposalDoesNotSucceedWithoutQuorum      PASS
package total                                13.932 s
```

Important interpretation: majority completion does not mean every follower has
already applied the entry. The test waits for eventual follower convergence.

## Proposal baseline

Command:

```bash
go test ./proof \
  -run '^$' \
  -bench '^BenchmarkThreeReplicaSyncProposal$' \
  -benchtime=100x \
  -count=3
```

Raw Go benchmark samples:

```text
BenchmarkThreeReplicaSyncProposal-12  100  23449571 ns/op  357505 B/op  225 allocs/op
BenchmarkThreeReplicaSyncProposal-12  100  25201347 ns/op  349410 B/op  209 allocs/op
BenchmarkThreeReplicaSyncProposal-12  100  22953073 ns/op  340431 B/op  194 allocs/op
```

The median sample is about 23.45 ms per synchronous proposal. Each benchmark
sample creates three new hosts and runs sequential proposals. Setup is outside
the timed section. Automatic snapshots are disabled in this benchmark.

## What these results do and do not establish

They establish that the pinned revision can run the basic Raft, apply, quorum
loss, snapshot, and reopen paths required for deeper evaluation.

They do not prove power-loss durability, cross-machine latency, group commit,
snapshot transfer, many-group density, per-volume binding, disk amplification,
or behavior under packet loss and slow disks. Those measurements remain
mandatory if a supportable Dragonboat v4 release is evaluated later.

Because the candidate fails the mandatory release-support gate, G1 deliberately
does not spend further engineering effort presenting those missing measurements
as if the candidate could be approved by benchmark score.
