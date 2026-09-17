package io.github.indreshgahoi.queue.metadata.adapter.in.web;

final class OpenApiExamples {
    static final String QUEUE = """
            {
              "tenantId": "acme",
              "queueName": "orders",
              "queueId": "f8a82bd2-f94d-4de5-8df7-66161975f35b",
              "generationId": "653af9a3-36ba-47f5-bd65-209d6b6c78c2",
              "partitionCount": 1,
              "replicationFactor": 3,
              "lifecycleState": "PROVISIONING",
              "metadataVersion": 0,
              "createdAt": "2026-09-01T12:00:00Z",
              "updatedAt": "2026-09-01T12:00:00Z"
            }
            """;

    static final String QUEUE_LIST = "[" + QUEUE + "]";

    static final String PROBLEM = """
            {
              "type": "urn:distributed-queue:queue-not-found",
              "title": "Not Found",
              "status": 404,
              "detail": "Queue acme/missing was not found",
              "instance": "/api/v1/tenants/acme/queues/missing"
            }
            """;

    private OpenApiExamples() {
    }
}
