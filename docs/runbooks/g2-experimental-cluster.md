# Experimental G2 Three-Node Cluster

This runbook starts one queue partition replicated across three queue-node
processes. It is for G2 development only. The cluster uses the deferred
Dragonboat v4 revision and does not carry a production durability guarantee.

## Start

```bash
docker compose -f compose.g2.yaml up --detach --build
```

Each node exposes a local HTTP port:

| Replica | HTTP | Raft inside Compose |
|---|---|---|
| 1 | `localhost:18101` | `queue-node-1:63001` |
| 2 | `localhost:18102` | `queue-node-2:63001` |
| 3 | `localhost:18103` | `queue-node-3:63001` |

Find the elected leader:

```bash
curl --fail http://localhost:18101/health/ready
curl --fail http://localhost:18102/health/ready
curl --fail http://localhost:18103/health/ready
```

Every ready response reports `replicaId`, `leaderId`, and `term`. Send customer
operations to the port whose `replicaId` equals `leaderId`. Automatic gateway
routing is a later milestone.

## Publish and receive

The HTTP API represents message bytes as standard JSON base64.

```bash
curl --request POST http://localhost:18101/v1/messages \
  --header 'Content-Type: application/json' \
  --data '{
    "payload": "aGVsbG8=",
    "producerRequestId": "example-publish-1"
  }'
```

Replace `18101` with the elected leader's port.

```bash
curl --request POST http://localhost:18101/v1/messages/receive \
  --header 'Content-Type: application/json' \
  --data '{"visibilityTimeoutMillis":30000}'
```

The response contains a signed `receiptHandle`. Acknowledge it with:

```bash
curl --request POST http://localhost:18101/v1/messages/ack \
  --header 'Content-Type: application/json' \
  --data '{"receiptHandle":"REPLACE_WITH_RECEIPT_HANDLE"}'
```

Or request redelivery:

```bash
curl --request POST http://localhost:18101/v1/messages/nack \
  --header 'Content-Type: application/json' \
  --data '{
    "receiptHandle": "REPLACE_WITH_RECEIPT_HANDLE",
    "retryDelayMillis": 1000
  }'
```

Only the current leader runs the background due-transition cycle. Lease expiry
and delayed retry are committed as ordinary Raft commands; follower observations
or duplicate cycles cannot directly mutate queue state.

## Verify leader failover

After identifying the leader, stop only that service. For example:

```bash
docker compose -f compose.g2.yaml stop queue-node-2
```

Query the two remaining readiness endpoints until they report a new common
`leaderId`. The surviving majority should accept another publish. With only one
replica running, mutations must fail or time out rather than report success.

## Restart versus fresh bootstrap

The Compose file supplies the immutable initial member set. Dragonboat records
that bootstrap set in each replica's durable LogDB and validates it on restart.
The same Compose configuration can therefore reopen the named volumes.

For a direct binary restart, `--bootstrap=false` starts from existing local
storage without requiring `--members`. It must not be used for an empty store.

## Stop

```bash
docker compose -f compose.g2.yaml down
```

Named volumes are intentionally retained for restart testing. Delete them only
when you explicitly want a new queue lineage and empty Raft history:

```bash
docker compose -f compose.g2.yaml down --volumes
```
