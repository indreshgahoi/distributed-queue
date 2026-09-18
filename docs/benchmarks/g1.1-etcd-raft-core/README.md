# G1.1 etcd/raft Core Baseline

## Purpose

This is a lower-bound allocation and construction baseline for 1,000 local
`RawNode` instances. It is not a queue-node density result. The measured core
has no disk WAL, transport connections, timers, proposal waiters, snapshots,
payload state, metrics, or per-volume runtime.

## Environment

| Item | Value |
|---|---|
| Date | 2026-09-18 |
| CPU | Intel Core i7-8700, 6 cores / 12 logical CPUs |
| OS / architecture | Linux / amd64 |
| Go | 1.27.1 |
| Dependency | `go.etcd.io/raft/v3 v3.7.0` |
| Storage | `raft.MemoryStorage` |
| Local replica membership | one voter per constructed group |

## Command

```bash
cd tools/etcd-raft-proof
go test ./proof \
  -run '^$' \
  -bench BenchmarkCreateOneThousandLocalRawNodes \
  -benchmem \
  -count=5
```

## Raw result

```text
BenchmarkCreateOneThousandLocalRawNodes-12  84  16732229 ns/op  6888190 B/op  148034 allocs/op
BenchmarkCreateOneThousandLocalRawNodes-12  57  17618097 ns/op  6887703 B/op  148032 allocs/op
BenchmarkCreateOneThousandLocalRawNodes-12  86  15900930 ns/op  6887967 B/op  148034 allocs/op
BenchmarkCreateOneThousandLocalRawNodes-12  87  16246723 ns/op  6887754 B/op  148033 allocs/op
BenchmarkCreateOneThousandLocalRawNodes-12  73  15089084 ns/op  6887777 B/op  148033 allocs/op
```

The median construction time is about 16.25 ms per 1,000 groups. Allocations
are about 6.89 MB and 148,033 objects per 1,000 groups, or roughly 6.9 KB and
148 allocations per local core instance during construction.

## Interpretation

The core itself is small enough to continue with a shared Multi-Raft host. This
does not prove that 1,000 production groups fit on a node. The project-owned
host will add the dominant resources: retained log indexes, payload state,
proposal waiters, tick scheduling, peer queues, encryption, metrics, snapshots,
and file-backed storage.

The next density gate must measure the complete host at 1,000 and 10,000 idle
groups, plus active-group mixes, and report retained heap, goroutines, file
descriptors, idle CPU, startup recovery, disk amplification, and tail latency.
