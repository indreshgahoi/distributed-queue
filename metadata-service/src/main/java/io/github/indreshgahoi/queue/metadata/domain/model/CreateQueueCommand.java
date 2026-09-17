package io.github.indreshgahoi.queue.metadata.domain.model;

public record CreateQueueCommand(
        String tenantId,
        String queueName,
        String idempotencyKey,
        int replicationFactor
) {
    public static final int DEFAULT_REPLICATION_FACTOR = 3;
    public static final int MAX_REPLICATION_FACTOR = 7;

    public CreateQueueCommand(
            String tenantId,
            String queueName,
            String idempotencyKey
    ) {
        this(
                tenantId,
                queueName,
                idempotencyKey,
                DEFAULT_REPLICATION_FACTOR
        );
    }

    public CreateQueueCommand {
        QueueDescriptor.requireText(tenantId, "tenantId");
        QueueDescriptor.requireText(queueName, "queueName");
        QueueDescriptor.requireText(idempotencyKey, "idempotencyKey");
        if (replicationFactor < 1
                || replicationFactor > MAX_REPLICATION_FACTOR) {
            throw new IllegalArgumentException(
                    "replicationFactor must be between 1 and "
                            + MAX_REPLICATION_FACTOR
            );
        }
    }
}
