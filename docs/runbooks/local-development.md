# Local Development Runbook

## Purpose

This runbook covers local build, startup, API testing, configuration, reset, and
common troubleshooting. It describes a trusted development environment, not a
production deployment.

## Prerequisites

- Java 21;
- Maven 3;
- Docker Engine or Docker Desktop;
- Docker Compose v2 (`docker compose version`);
- local ports `5432`, `8080`, `8081`, `8082`, `8083`, and `8084` available.

Run commands from the repository root.

## Services and ports

| Service | Port | Purpose |
|---|---:|---|
| PostgreSQL | 5432 | Metadata persistence |
| Metadata service | 8080 | Queue lifecycle and placement control plane |
| Queue node 1 | 8081 | Bootstrap data plane and replica storage |
| Queue gateway | 8082 | Stable customer message API |
| Queue node 2 | 8083 | Replica storage and trusted internal APIs |
| Queue node 3 | 8084 | Replica storage and trusted internal APIs |

## Start the complete environment

```bash
docker compose up --detach --build
docker compose ps
```

Compose waits for PostgreSQL health before starting dependent services. Flyway
applies metadata schema migrations during metadata-service startup.

Expected state:

- `postgres` is `healthy`;
- `metadata-service`, all three queue nodes, and `queue-gateway` are `Up`.

Follow logs:

```bash
docker compose logs --follow metadata-service
docker compose logs --follow queue-node
docker compose logs --follow queue-node-2 queue-node-3
docker compose logs --follow queue-gateway
```

Verify metadata health:

```bash
curl --fail http://localhost:8080/actuator/health
```

The response should contain `"status":"UP"`.

## Swagger UI

| API | URL |
|---|---|
| Metadata | [http://localhost:8080/swagger-ui.html](http://localhost:8080/swagger-ui.html) |
| Queue node | [http://localhost:8081/swagger-ui.html](http://localhost:8081/swagger-ui.html) |
| Queue node 2 | [http://localhost:8083/swagger-ui.html](http://localhost:8083/swagger-ui.html) |
| Queue node 3 | [http://localhost:8084/swagger-ui.html](http://localhost:8084/swagger-ui.html) |
| Queue gateway | [http://localhost:8082/swagger-ui.html](http://localhost:8082/swagger-ui.html) |

OpenAPI documents are exposed at `/v3/api-docs` on each service.

## Create and provision a queue

```bash
curl --request POST \
  http://localhost:8080/api/v1/tenants/acme/queues \
  --header 'Content-Type: application/json' \
  --header 'Idempotency-Key: create-orders-001' \
  --data '{"queueName":"orders","replicationFactor":3}'
```

The response normally starts in `PROVISIONING`. The three queue nodes
independently materialize their assigned lineage-bound storage. Metadata marks
the queue `ACTIVE` only after all three complete fenced provisioning. Queue
creation success does not mean storage is immediately ready.

Read or list queue descriptors:

```bash
curl http://localhost:8080/api/v1/tenants/acme/queues/orders
curl http://localhost:8080/api/v1/tenants/acme/queues
```

Repeating the create request with the same idempotency key and body returns the
same queue identity. Reusing the key for a different request is rejected.

## Exercise the customer data plane

Use the `queueId` returned by queue creation:

```bash
QUEUE_ID='<queue-id-from-create-response>'
```

Publish and receive:

```bash
curl --request POST \
  "http://localhost:8082/v1/queues/${QUEUE_ID}/messages" \
  --header 'Content-Type: application/json' \
  --data '{"payload":"process-order-123"}'

curl --request POST \
  "http://localhost:8082/v1/queues/${QUEUE_ID}/messages/receive"
```

Use the returned receipt handle to ACK or NACK:

```bash
RECEIPT_HANDLE='<receipt-handle-from-receive-response>'

curl --request POST \
  "http://localhost:8082/v1/queues/${QUEUE_ID}/messages/${RECEIPT_HANDLE}/ack"

curl --request POST \
  "http://localhost:8082/v1/queues/${QUEUE_ID}/messages/${RECEIPT_HANDLE}/nack" \
  --header 'Content-Type: application/json' \
  --data '{"retryDelay":"PT30S"}'
```

| Condition | Response |
|---|---|
| Empty receive | `204 No Content` |
| No READY route | `503 Service Unavailable` |
| Payload too large | `413 Payload Too Large` |
| Retained capacity exhausted | `429 Too Many Requests` |

The gateway performs one metadata lookup and one node call. It does not retry
an ambiguous mutation automatically.

## Build and test

```bash
mvn clean test
```

Build without tests:

```bash
mvn clean package -DskipTests
```

PostgreSQL integration tests use Testcontainers and are skipped when Docker is
unavailable.

## Run the Go control-plane foundation

This stack is separate from the Java baseline. It starts PostgreSQL with logical
WAL, etcd, the Go metadata API, and the non-polling CDC projector:

```bash
docker compose -f compose.go.yaml up --detach --build
docker compose -f compose.go.yaml ps
```

Create a queue through the Go metadata API on port `18080`:

```bash
curl --fail http://localhost:18080/health/ready

curl --request POST \
  http://localhost:18080/v1/tenants/acme/queues \
  --header 'Content-Type: application/json' \
  --header 'Idempotency-Key: create-orders-001' \
  --data '{"name":"orders","partitionCount":4}'
```

Queue creation commits the queue row, idempotency record, and outbox event in
one PostgreSQL transaction. The projector consumes `pgoutput` from a durable
replication slot and updates etcd. It does not poll metadata tables.

Inspect projected queue state:

```bash
docker compose -f compose.go.yaml exec etcd \
  /usr/local/bin/etcdctl get /dq/v1/projections/current/queues/ --prefix
```

Run Go checks with Go 1.25:

```bash
go test ./...
go vet ./...
```

The adapter integration tests are enabled with `DQ_TEST_POSTGRES_URL`,
`DQ_TEST_CDC_POSTGRES_URL`, and `DQ_TEST_ETCD_ENDPOINT`.

Stop the Go stack while preserving its volumes:

```bash
docker compose -f compose.go.yaml down
```

Add `--volumes` only when you deliberately want to delete its local PostgreSQL
and etcd state.

## Run Java on the host

Start only PostgreSQL in Docker, then run a service from Maven:

```bash
docker compose up --detach postgres
mvn -pl metadata-service -am install -DskipTests
mvn -pl metadata-service spring-boot:run
```

Use the equivalent module command for `queue-node` or `queue-gateway`.

## Configuration

### Metadata service

| Variable | Local default |
|---|---|
| `METADATA_DATABASE_URL` | `jdbc:postgresql://localhost:5432/queue_metadata` |
| `METADATA_DATABASE_USER` | `queue` |
| `METADATA_DATABASE_PASSWORD` | `queue` |
| `METADATA_HTTP_PORT` | `8080` |

### Queue node

| Variable | Local default/example |
|---|---|
| `QUEUE_NODE_ID` | `local-node-1` |
| `QUEUE_NODE_ENDPOINT` | `http://localhost:8081` |
| `METADATA_SERVICE_URL` | `http://localhost:8080` |
| `QUEUE_STORAGE_ROOT` | `./queue-data` |
| `QUEUE_NODE_PORT` | `8081` |
| `NODE_REGISTRATION_LEASE_DURATION` | `PT30S` |
| `NODE_HEARTBEAT_DELAY` | `PT10S` |
| `PROVISIONING_LEASE_DURATION` | `PT30S` |
| `QUEUE_NODE_HTTP_CONNECT_TIMEOUT` | `PT2S` |
| `QUEUE_NODE_HTTP_REQUEST_TIMEOUT` | `PT5S` |
| `PROVISIONING_POLL_DELAY` | `PT1S` |
| `RUNTIME_PARTITION_POLL_DELAY` | `PT1S` |
| `QUEUE_WAL_SEGMENT_BYTES` | `16777216` |
| `QUEUE_MAX_MESSAGE_BYTES` | `262144` |
| `QUEUE_MAX_RETAINED_MESSAGES` | `100000` |
| `QUEUE_MAX_RETAINED_BYTES` | `1073741824` |

### Queue gateway

| Variable | Local default/example |
|---|---|
| `QUEUE_GATEWAY_PORT` | `8082` |
| `METADATA_SERVICE_URL` | `http://localhost:8080` |
| `GATEWAY_CONNECT_TIMEOUT` | `PT2S` |
| `GATEWAY_REQUEST_TIMEOUT` | `PT10S` |

Container defaults may use Compose service names instead of `localhost`. Check
`compose.yaml` before overriding them.

## Trusted internal APIs

These endpoint families are intentionally unauthenticated for local learning
and must not be exposed to an untrusted network:

```text
/internal/v1/provisioning
/internal/v1/nodes
/internal/v1/placements
/internal/v1/runtime
/internal/v1/replicas
```

Useful runtime inspection:

```text
GET http://localhost:8080/internal/v1/runtime/partitions
GET http://localhost:8081/internal/v1/runtime/partitions
```

Protocol diagrams:

- [Provisioning sequence](../diagrams/provisioning-claim-sequence.md)
- [Provisioning decision flow](../diagrams/provisioning-claim-flow.md)
- [Runtime lifecycle](../diagrams/runtime-partition-lifecycle.md)
- [Stable routing sequence](../diagrams/stable-routing-sequence.md)
- [Replica catch-up sequence](../diagrams/replica-catch-up-sequence.md)

## Stop and reset

Preserve volumes:

```bash
docker compose down
```

Discard all Compose-managed metadata and queue storage:

```bash
docker compose down --volumes
```

The second command permanently deletes local development data.

## Troubleshooting

### `UnknownHostException: postgres`

Recreate the Compose network without removing volumes:

```bash
docker compose down
docker compose up --detach --build
docker compose ps
```

### PostgreSQL is not ready

```bash
docker compose ps
docker compose logs postgres
docker compose logs metadata-service
```

Confirm PostgreSQL is `healthy` before diagnosing Flyway or application code.

### A queue remains in `PROVISIONING`

```bash
docker compose logs queue-node
curl http://localhost:8080/internal/v1/runtime/partitions
curl http://localhost:8081/internal/v1/runtime/partitions
```

Check node registration, provisioning-claim fencing, storage permissions, and
runtime activation logs.

### Legacy Compose command

If `docker compose` is unavailable but `docker-compose version` succeeds, use
`docker-compose` in place of `docker compose` throughout this runbook.
