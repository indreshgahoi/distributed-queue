package io.github.indreshgahoi.queue.metadata.adapter.in.web;

import io.github.indreshgahoi.queue.metadata.application.port.in.QueueCatalogUseCase;
import io.github.indreshgahoi.queue.metadata.domain.exception.QueueNotFoundException;
import io.github.indreshgahoi.queue.metadata.domain.model.CreateQueueCommand;
import io.github.indreshgahoi.queue.metadata.domain.model.QueueDescriptor;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import java.net.URI;
import java.util.List;

@Validated
@RestController
@RequestMapping("/api/v1/tenants/{tenantId}/queues")
@Tag(
        name = "Queue metadata",
        description = "Create and inspect tenant-scoped queue identities"
)
class QueueMetadataController {
    private final QueueCatalogUseCase service;

    QueueMetadataController(QueueCatalogUseCase service) {
        this.service = service;
    }

    @PostMapping
    @Operation(
            summary = "Create a queue",
            description = "Creates a queue in PROVISIONING state. Reusing the "
                    + "same idempotency key and request returns the same queue."
    )
    @ApiResponses({
            @ApiResponse(
                    responseCode = "201",
                    description = "Queue identity created",
                    content = @Content(
                            schema = @Schema(
                                    implementation = QueueResponse.class
                            ),
                            examples = @ExampleObject(
                                    value = OpenApiExamples.QUEUE
                            )
                    )
            ),
            @ApiResponse(
                    responseCode = "400",
                    description = "Invalid tenant, idempotency key, or body"
            ),
            @ApiResponse(
                    responseCode = "409",
                    description = "Queue name or idempotency conflict"
            ),
            @ApiResponse(
                    responseCode = "503",
                    description = "Metadata database unavailable"
            )
    })
    public ResponseEntity<QueueResponse> createQueue(
            @Parameter(example = "acme")
            @PathVariable @NotBlank @Size(max = 255) String tenantId,
            @Parameter(
                    description = "Retry token unique within the tenant",
                    example = "create-orders-001"
            )
            @RequestHeader("Idempotency-Key")
            @NotBlank @Size(max = 255) String idempotencyKey,
            @Valid @RequestBody CreateQueueRequest request
    ) {
        QueueDescriptor created = service.createQueue(
                new CreateQueueCommand(
                        tenantId,
                        request.queueName(),
                        idempotencyKey,
                        request.effectiveReplicationFactor()
                )
        );
        URI location = ServletUriComponentsBuilder
                .fromCurrentRequest()
                .path("/{queueName}")
                .buildAndExpand(created.queueName())
                .toUri();
        return ResponseEntity.created(location)
                .body(QueueResponse.from(created));
    }

    @GetMapping("/{queueName}")
    @Operation(summary = "Get a queue")
    @ApiResponses({
            @ApiResponse(
                    responseCode = "200",
                    description = "Queue metadata",
                    content = @Content(
                            schema = @Schema(
                                    implementation = QueueResponse.class
                            ),
                            examples = @ExampleObject(
                                    value = OpenApiExamples.QUEUE
                            )
                    )
            ),
            @ApiResponse(
                    responseCode = "404",
                    description = "Queue not found",
                    content = @Content(
                            examples = @ExampleObject(
                                    value = OpenApiExamples.PROBLEM
                            )
                    )
            )
    })
    public QueueResponse getQueue(
            @Parameter(example = "acme") @PathVariable String tenantId,
            @Parameter(example = "orders") @PathVariable String queueName
    ) {
        return service.getQueue(tenantId, queueName)
                .map(QueueResponse::from)
                .orElseThrow(() ->
                        new QueueNotFoundException(tenantId, queueName)
                );
    }

    @GetMapping
    @Operation(summary = "List queues in a tenant")
    @ApiResponse(
            responseCode = "200",
            description = "Tenant queue metadata",
            content = @Content(
                    array = @ArraySchema(
                            schema = @Schema(
                                    implementation = QueueResponse.class
                            )
                    ),
                    examples = @ExampleObject(
                            value = OpenApiExamples.QUEUE_LIST
                    )
            )
    )
    public List<QueueResponse> listQueues(
            @Parameter(example = "acme") @PathVariable String tenantId
    ) {
        return service.listQueues(tenantId)
                .stream()
                .map(QueueResponse::from)
                .toList();
    }

    @DeleteMapping("/{queueName}")
    @Operation(
            summary = "Begin deleting a queue",
            description = "Moves an ACTIVE queue to DELETING. Physical storage "
                    + "cleanup is completed by a future trusted provisioner."
    )
    @ApiResponses({
            @ApiResponse(
                    responseCode = "202",
                    description = "Deletion accepted",
                    content = @Content(
                            schema = @Schema(
                                    implementation = QueueResponse.class
                            ),
                            examples = @ExampleObject(
                                    value = OpenApiExamples.QUEUE
                            )
                    )
            ),
            @ApiResponse(
                    responseCode = "404",
                    description = "Queue not found"
            ),
            @ApiResponse(
                    responseCode = "409",
                    description = "Queue lifecycle does not permit deletion"
            )
    })
    public ResponseEntity<QueueResponse> deleteQueue(
            @Parameter(example = "acme") @PathVariable String tenantId,
            @Parameter(example = "orders") @PathVariable String queueName
    ) {
        QueueDescriptor deleting = service.beginDeleteQueue(
                tenantId,
                queueName
        );
        return ResponseEntity.accepted()
                .body(QueueResponse.from(deleting));
    }
}
