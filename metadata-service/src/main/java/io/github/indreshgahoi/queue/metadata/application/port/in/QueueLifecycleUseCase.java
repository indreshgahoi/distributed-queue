package io.github.indreshgahoi.queue.metadata.application.port.in;

import io.github.indreshgahoi.queue.metadata.domain.model.QueueDescriptor;

public interface QueueLifecycleUseCase {

    QueueDescriptor completeDeletion(QueueDescriptor expected);

    QueueDescriptor failDeletion(QueueDescriptor expected);

    QueueDescriptor retryDeletion(QueueDescriptor expected);
}
