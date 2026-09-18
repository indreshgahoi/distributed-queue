# G2 Replicated Queue-Command Baseline

## Scope

This benchmark measures the experimental G2 queue adapter, including JSON
command encoding, three-replica Raft proposal, deterministic queue apply, and
result decoding. It is not a production capacity claim.

| Field | Value |
|---|---|
| Date | 2026-09-18 |
| Candidate | Dragonboat v4 commit `076c7f6497dcc18880aed6323246d5079661942c` |
| Replicas | Three local `NodeHost` instances over TCP |
| Payload | Approximately 1 KiB |
| Operation | Sequential publish with a unique command and producer ID |
| Snapshot policy | Disabled during benchmark |
| Go | 1.27.1 linux/amd64 |
| CPU | Intel Core i7-8700, 6 cores / 12 threads |

Command:

```bash
cd tools/consensus-proof
go test ./queuegroup \
  -run '^$' \
  -bench '^BenchmarkReplicatedQueuePublish$' \
  -benchtime=100x \
  -count=3
```

Raw results:

```text
BenchmarkReplicatedQueuePublish-12  100  27019129 ns/op  388355 B/op  291 allocs/op
BenchmarkReplicatedQueuePublish-12  100  24998879 ns/op  390822 B/op  286 allocs/op
BenchmarkReplicatedQueuePublish-12  100  22897817 ns/op  377315 B/op  261 allocs/op
```

The median sample is approximately **25.00 ms per publish**. The benchmark is
sequential, uses localhost, and creates three independent durable stores on the
same development filesystem. It does not measure batching, group commit,
cross-host networking, concurrent producers, snapshot interference, or tail
latency under failure.

The high allocation count includes the library and JSON adapter path. It is a
future profiling input, not justification for optimization before the
consensus dependency is approved.
