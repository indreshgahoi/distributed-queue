# ADR 0032: Isolate the G2 Replicated Partition Until Consensus Approval

## Status

Accepted for experimental G2 evidence. Not accepted as a production dependency
or release guarantee.

## Context

G1 deferred Dragonboat v4 because its required API remains on an unstable
development line. G2 still needs executable evidence that the existing queue
state machine can run behind a real three-replica Raft group. Folding the
deferred library into the root Go module would contradict ADR 0031, while
waiting would leave the queue/Raft integration risks untested.

The important integration risks are independent of library release status:

- a Raft log contains internal and membership entries, while queue command
  indexes must remain contiguous;
- proposal results must be returned from deterministic apply;
- reads used to select a message must cross a linearizable consensus read;
- snapshot restore must reconstruct the complete queue domain state;
- retry after an ambiguous client response must not publish twice;
- loss of a majority must stop successful mutations.

## Decision

Implement G2 in the already isolated `tools/consensus-proof` Go module. The
root production module still has no Dragonboat dependency.

The adapter implements the project-owned `consensus.Group` interface and maps
one queue partition to one Dragonboat shard. It uses:

```text
queue lineage RaftGroupID -> Dragonboat ShardID
replica placement ID      -> Dragonboat ReplicaID
queue command             -> one Raft proposal
queue result              <- deterministic state-machine apply result
```

Raft indexes and queue logical indexes remain separate:

```text
Raft log
  membership entry     index 1
  internal entry       index 2
  queue publish        index 3  -> queue command index 1
  queue claim          index 4  -> queue command index 2
```

Using Raft index 3 as the first queue index would violate the domain's gap-free
apply invariant. The adapter therefore increments the logical command index
only when Dragonboat invokes the queue state machine with a queue command.

All customer mutation success comes from `SyncPropose` returning the result
created by deterministic apply. Advisory local reads exist only for convergence
tests and observability; customer operations use `SyncRead`.

Three independently runnable queue-node processes are provided through
`compose.g2.yaml`. Each replica owns a stable storage volume and a distinct
Raft address. The HTTP surface is intentionally narrow: publish, receive, ACK,
NACK, liveness, and readiness.

## Verified behavior

Automated integration tests prove:

- publish, claim, NACK, delayed-ready, lease-expiry, DLQ, redelivery, and ACK
  cross a three-replica group;
- every available replica eventually applies the same queue command count;
- an acknowledged publish survives leader failure;
- retrying the same command after a lost response returns the recorded result
  without retaining a second message;
- snapshot plus retained Raft log restores all three replicas after restart;
- a proposal cannot succeed after two replicas are unavailable.

A three-container smoke test additionally proved leader election, publish,
receive, leader shutdown, new leader election, and publish through the new
leader.

## Explicit non-guarantees

This implementation does not override ADR 0031. It is not evidence of:

- a supported Dragonboat upgrade or security-response policy;
- power-loss durability at the client response boundary;
- snapshot transfer to a new learner;
- dynamic membership changes;
- many-group scheduling, fairness, or stable multi-volume placement;
- authenticated or encrypted client and Raft traffic;
- a production-compatible API or operational SLO.

G2 can be promoted from experimental only after G1.1 approves a supportable
consensus implementation and this adapter is moved behind the root production
module boundary with the remaining durability tests.

## Consequences

### Positive

- queue/Raft integration risks are executable rather than speculative;
- the production dependency boundary remains honest;
- logical queue indexing is protected from Raft implementation details;
- the same integration tests can evaluate a later supported candidate.

### Negative

- the experimental module imports the root module's internal queue packages;
- the adapter must be migrated or replaced after the dependency decision;
- two local run paths exist and must be labeled clearly.

## References

- [ADR 0031](0031-defer-dragonboat-v4.md)
- [G2 runbook](../runbooks/g2-experimental-cluster.md)
- [G2 provisioning and replication diagrams](../diagrams/g2-provisioning-and-replication.md)
- [G2 benchmark evidence](../benchmarks/g2-replicated-partition/README.md)
