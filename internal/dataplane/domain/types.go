package domain

import "errors"

const CommandVersion uint16 = 1

var (
	ErrCapacity          = errors.New("queue capacity exceeded")
	ErrCommandGap        = errors.New("command index is not the next applied index")
	ErrInvalidCommand    = errors.New("invalid queue command")
	ErrLineageMismatch   = errors.New("command lineage does not match partition")
	ErrMessageTooLarge   = errors.New("message exceeds maximum size")
	ErrSnapshotCorrupt   = errors.New("snapshot is inconsistent")
	ErrUnsupportedSchema = errors.New("unsupported command schema version")
)

type Status string

const (
	StatusDelayed    Status = "DELAYED"
	StatusReady      Status = "READY"
	StatusInFlight   Status = "IN_FLIGHT"
	StatusDeadLetter Status = "DEAD_LETTER"
)

type CommandType string

const (
	CommandPublish          CommandType = "PUBLISH"
	CommandClaim            CommandType = "CLAIM"
	CommandAcknowledge      CommandType = "ACKNOWLEDGE"
	CommandNegativeAck      CommandType = "NEGATIVE_ACKNOWLEDGE"
	CommandExpireLease      CommandType = "EXPIRE_LEASE"
	CommandMakeDelayedReady CommandType = "MAKE_DELAYED_READY"
	CommandMoveDeadLetter   CommandType = "MOVE_TO_DEAD_LETTER"
)

type ResultCode string

const (
	ResultApplied   ResultCode = "APPLIED"
	ResultDuplicate ResultCode = "DUPLICATE"
	ResultNotFound  ResultCode = "NOT_FOUND"
	ResultRejected  ResultCode = "REJECTED"
	ResultStale     ResultCode = "STALE"
)

type Lineage struct {
	QueueID      string `json:"queueId"`
	GenerationID string `json:"generationId"`
	PartitionID  uint32 `json:"partitionId"`
	RaftGroupID  uint64 `json:"raftGroupId"`
}

func (l Lineage) valid() bool {
	return l.QueueID != "" && l.GenerationID != "" && l.RaftGroupID != 0
}

type Configuration struct {
	MaxDeliveryAttempts int
	MaxMessageBytes     int
	MaxRetainedMessages int
	MaxRetainedBytes    int64
	MaxDeduplicationIDs int
	MaxCommandResults   int
}

func DefaultConfiguration() Configuration {
	return Configuration{
		MaxDeliveryAttempts: 3,
		MaxMessageBytes:     256 * 1024,
		MaxRetainedMessages: 100_000,
		MaxRetainedBytes:    1024 * 1024 * 1024,
		MaxDeduplicationIDs: 100_000,
		MaxCommandResults:   100_000,
	}
}

func (c Configuration) valid() bool {
	return c.MaxDeliveryAttempts > 0 &&
		c.MaxMessageBytes > 0 &&
		c.MaxRetainedMessages > 0 &&
		c.MaxRetainedBytes > 0 &&
		c.MaxDeduplicationIDs > 0 &&
		c.MaxCommandResults > 0
}

type Command struct {
	SchemaVersion uint16        `json:"schemaVersion"`
	CommandID     string        `json:"commandId"`
	Lineage       Lineage       `json:"lineage"`
	Type          CommandType   `json:"type"`
	Publish       *Publish      `json:"publish,omitempty"`
	Claim         *Claim        `json:"claim,omitempty"`
	Acknowledge   *Acknowledge  `json:"acknowledge,omitempty"`
	NegativeAck   *NegativeAck  `json:"negativeAcknowledge,omitempty"`
	ExpireLease   *ExpireLease  `json:"expireLease,omitempty"`
	DelayedReady  *DelayedReady `json:"delayedReady,omitempty"`
	DeadLetter    *DeadLetter   `json:"deadLetter,omitempty"`
}

type Publish struct {
	MessageID         string `json:"messageId"`
	ProducerRequestID string `json:"producerRequestId,omitempty"`
	Payload           []byte `json:"payload"`
	AvailableAt       int64  `json:"availableAt"`
	ObservedAt        int64  `json:"observedAt"`
}

type Claim struct {
	MessageID       string `json:"messageId"`
	ExpectedAttempt int    `json:"expectedAttempt"`
	LeaseToken      string `json:"leaseToken"`
	LeaseDeadline   int64  `json:"leaseDeadline"`
	ObservedAt      int64  `json:"observedAt"`
}

type Acknowledge struct {
	MessageID  string `json:"messageId"`
	LeaseToken string `json:"leaseToken"`
	Attempt    int    `json:"attempt"`
	ObservedAt int64  `json:"observedAt"`
}

type NegativeAck struct {
	MessageID   string `json:"messageId"`
	LeaseToken  string `json:"leaseToken"`
	Attempt     int    `json:"attempt"`
	AvailableAt int64  `json:"availableAt"`
	ObservedAt  int64  `json:"observedAt"`
}

type ExpireLease struct {
	MessageID        string `json:"messageId"`
	LeaseToken       string `json:"leaseToken"`
	Attempt          int    `json:"attempt"`
	ExpectedDeadline int64  `json:"expectedDeadline"`
	ObservedAt       int64  `json:"observedAt"`
}

type DelayedReady struct {
	MessageID           string `json:"messageId"`
	ExpectedAvailableAt int64  `json:"expectedAvailableAt"`
	ObservedAt          int64  `json:"observedAt"`
}

type DeadLetter struct {
	MessageID       string `json:"messageId"`
	ExpectedAttempt int    `json:"expectedAttempt"`
	ObservedAt      int64  `json:"observedAt"`
}

type Result struct {
	Code       ResultCode `json:"code"`
	MessageID  string     `json:"messageId,omitempty"`
	Payload    []byte     `json:"payload,omitempty"`
	Attempt    int        `json:"attempt,omitempty"`
	LeaseToken string     `json:"leaseToken,omitempty"`
}

type Message struct {
	ID               string
	Payload          []byte
	Status           Status
	AvailableAt      int64
	NextAttempt      int
	DeliveryAttempt  int
	ActiveLeaseToken string
	LeaseDeadline    int64
}

func cloneBytes(value []byte) []byte {
	return append([]byte(nil), value...)
}
