# G1.1 etcd/raft Core Proof

This nested module evaluates the exact consensus core accepted by
[ADR 0033](../../docs/adr/0033-accept-etcd-raft-core.md):

```text
go.etcd.io/raft/v3 v3.7.0
```

It intentionally does not import the dependency into the root production
module yet. The production adapter first needs the project-owned durable log,
transport, snapshot-transfer, proposal-tracking, and Multi-Raft host described
by the ADR.

## What the proof establishes

- three deterministic `RawNode` replicas elect a leader;
- normal entries converge after quorum commit;
- proposal submission is not mistaken for commit or application;
- no entry applies while two of three replicas are unavailable;
- snapshot plus retained Raft storage reconstructs every replica;
- a restarted group can elect a leader and continue committing;
- the `Ready` processing order is explicit in executable code.

The harness persists each `Ready` batch to `MemoryStorage` before releasing its
messages and applies only `CommittedEntries`. This verifies the host contract,
not power-loss durability.

## What the proof does not establish

- disk WAL durability, torn-write repair, or group commit;
- peer transport, authentication, batching, or snapshot transfer;
- proposal completion across process restart;
- stable multi-volume placement;
- bounded resource use in a complete Multi-Raft host;
- production latency or throughput.

Those are project-owned responsibilities precisely because etcd/raft is a
consensus core rather than a complete distributed-storage runtime.

## Run

```bash
cd tools/etcd-raft-proof
go test ./...
go vet ./...
go test ./proof \
  -run '^$' \
  -bench BenchmarkCreateOneThousandLocalRawNodes \
  -benchmem \
  -count=5
```

Recorded results and interpretation are in
[`docs/benchmarks/g1.1-etcd-raft-core/`](../../docs/benchmarks/g1.1-etcd-raft-core/README.md).
