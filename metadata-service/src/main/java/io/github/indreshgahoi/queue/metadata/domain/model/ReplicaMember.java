package io.github.indreshgahoi.queue.metadata.domain.model;

public record ReplicaMember(
        String nodeId,
        ReplicaMemberRole role,
        int ordinal
) {
    public ReplicaMember {
        QueueDescriptor.requireText(nodeId, "nodeId");
        if (role == null) {
            throw new NullPointerException("role");
        }
        if (ordinal < 0) {
            throw new IllegalArgumentException("ordinal must not be negative");
        }
    }
}
