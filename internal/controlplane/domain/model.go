package domain

import (
	"errors"
	"time"
)

const (
	QueueEventSchemaVersion  uint16 = 1
	DefaultReplicationFactor        = 3
)

var (
	ErrIdempotencyConflict  = errors.New("idempotency key was used for a different request")
	ErrInvalidQueue         = errors.New("invalid queue configuration")
	ErrQueueExists          = errors.New("live queue already exists for tenant and name")
	ErrUnsupportedEvent     = errors.New("unsupported outbox event")
	ErrInsufficientCapacity = errors.New("insufficient failure-domain-safe capacity")
)

type QueueLifecycle string

const (
	QueueProvisioning QueueLifecycle = "PROVISIONING"
	QueueActive       QueueLifecycle = "ACTIVE"
	QueueDeleting     QueueLifecycle = "DELETING"
	QueueDeleted      QueueLifecycle = "DELETED"
)

type QueueGeneration struct {
	TenantID                string         `json:"tenantId"`
	QueueID                 string         `json:"queueId"`
	QueueName               string         `json:"queueName"`
	GenerationID            string         `json:"generationId"`
	Lifecycle               QueueLifecycle `json:"lifecycle"`
	PartitionCount          uint32         `json:"partitionCount"`
	ReplicationFactor       uint32         `json:"replicationFactor"`
	RoutingAlgorithmVersion uint16         `json:"routingAlgorithmVersion"`
	RoutingSeed             string         `json:"routingSeed"`
	MetadataVersion         uint64         `json:"metadataVersion"`
	CreatedAt               time.Time      `json:"createdAt"`
	UpdatedAt               time.Time      `json:"updatedAt"`
}

type CreateQueue struct {
	TenantID          string
	QueueName         string
	IdempotencyKey    string
	PartitionCount    uint32
	ReplicationFactor uint32
	RequestHash       string
	QueueID           string
	GenerationID      string
	RoutingSeed       string
	EventID           string
	Now               time.Time
}

type OutboxEvent struct {
	EventID          string    `json:"eventId"`
	AggregateType    string    `json:"aggregateType"`
	AggregateID      string    `json:"aggregateId"`
	AggregateVersion uint64    `json:"aggregateVersion"`
	EventType        string    `json:"eventType"`
	SchemaVersion    uint16    `json:"schemaVersion"`
	OccurredAt       time.Time `json:"occurredAt"`
	Payload          []byte    `json:"payload"`
}

func (command CreateQueue) Valid() bool {
	return command.TenantID != "" && command.QueueName != "" && command.IdempotencyKey != "" &&
		command.PartitionCount > 0 && command.ReplicationFactor > 0 &&
		command.QueueID != "" && command.GenerationID != "" && command.RoutingSeed != "" &&
		command.EventID != "" && command.RequestHash != "" && !command.Now.IsZero()
}

type Volume struct {
	VolumeID      string
	StorageClass  string
	Healthy       bool
	CapacityBytes int64
	ReservedBytes int64
	ActiveGroups  int
}

type Node struct {
	NodeID       string
	Region       string
	Zone         string
	Rack         string
	SessionAlive bool
	Draining     bool
	ActiveGroups int
	Volumes      []Volume
}

type ReplicaPlacement struct {
	NodeID   string
	VolumeID string
	Ordinal  uint32
}
