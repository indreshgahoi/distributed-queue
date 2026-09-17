package application

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"github.com/indreshgahoi/distributed-queue/internal/dataplane/consensus"
	"github.com/indreshgahoi/distributed-queue/internal/dataplane/domain"
	"github.com/indreshgahoi/distributed-queue/internal/dataplane/receipt"
)

var (
	ErrEmptyQueue     = errors.New("no ready message in partition")
	ErrReceiptLineage = errors.New("receipt belongs to another partition lineage")
)

type IDSource interface{ NewID() (string, error) }

type RandomIDSource struct{}

func (RandomIDSource) NewID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}

type Clock interface{ Now() time.Time }

type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now().UTC() }

type QueueService struct {
	group  consensus.Group
	signer *receipt.Signer
	ids    IDSource
	clock  Clock
}

func NewQueueService(group consensus.Group, signer *receipt.Signer, ids IDSource, clock Clock) *QueueService {
	return &QueueService{group: group, signer: signer, ids: ids, clock: clock}
}

type Delivery struct {
	MessageID     string
	Payload       []byte
	ReceiptHandle string
	Attempt       int
}

func (s *QueueService) Publish(ctx context.Context, payload []byte, producerRequestID string) (domain.Result, error) {
	messageID, err := s.ids.NewID()
	if err != nil {
		return domain.Result{}, err
	}
	commandID, err := s.ids.NewID()
	if err != nil {
		return domain.Result{}, err
	}
	now := s.clock.Now().UnixNano()
	return s.group.Propose(ctx, domain.Command{
		SchemaVersion: domain.CommandVersion,
		CommandID:     commandID,
		Lineage:       s.group.Lineage(),
		Type:          domain.CommandPublish,
		Publish: &domain.Publish{
			MessageID: messageID, ProducerRequestID: producerRequestID,
			Payload: append([]byte(nil), payload...), AvailableAt: now, ObservedAt: now,
		},
	})
}

func (s *QueueService) Receive(ctx context.Context, visibilityTimeout time.Duration) (Delivery, error) {
	if visibilityTimeout <= 0 {
		return Delivery{}, domain.ErrInvalidCommand
	}
	candidate, ok, err := s.group.NextReady(ctx)
	if err != nil {
		return Delivery{}, err
	}
	if !ok {
		return Delivery{}, ErrEmptyQueue
	}
	commandID, err := s.ids.NewID()
	if err != nil {
		return Delivery{}, err
	}
	leaseToken, err := s.ids.NewID()
	if err != nil {
		return Delivery{}, err
	}
	now := s.clock.Now()
	result, err := s.group.Propose(ctx, domain.Command{
		SchemaVersion: domain.CommandVersion,
		CommandID:     commandID,
		Lineage:       s.group.Lineage(),
		Type:          domain.CommandClaim,
		Claim: &domain.Claim{
			MessageID: candidate.ID, ExpectedAttempt: candidate.NextAttempt,
			LeaseToken: leaseToken, ObservedAt: now.UnixNano(),
			LeaseDeadline: now.Add(visibilityTimeout).UnixNano(),
		},
	})
	if err != nil {
		return Delivery{}, err
	}
	if result.Code != domain.ResultApplied {
		return Delivery{}, ErrEmptyQueue
	}
	lineage := s.group.Lineage()
	handle, err := s.signer.Sign(receipt.Claims{
		Version: receipt.Version, QueueID: lineage.QueueID, GenerationID: lineage.GenerationID,
		PartitionID: lineage.PartitionID, MessageID: result.MessageID,
		Attempt: result.Attempt, LeaseToken: result.LeaseToken,
	})
	if err != nil {
		return Delivery{}, err
	}
	return Delivery{
		MessageID: result.MessageID, Payload: result.Payload,
		ReceiptHandle: handle, Attempt: result.Attempt,
	}, nil
}

func (s *QueueService) Acknowledge(ctx context.Context, handle string) (domain.Result, error) {
	claims, err := s.signer.Verify(handle)
	if err != nil {
		return domain.Result{}, err
	}
	if err := validateReceiptLineage(claims, s.group.Lineage()); err != nil {
		return domain.Result{}, err
	}
	commandID, err := s.ids.NewID()
	if err != nil {
		return domain.Result{}, err
	}
	return s.group.Propose(ctx, domain.Command{
		SchemaVersion: domain.CommandVersion,
		CommandID:     commandID,
		Lineage:       s.group.Lineage(),
		Type:          domain.CommandAcknowledge,
		Acknowledge: &domain.Acknowledge{
			MessageID: claims.MessageID, LeaseToken: claims.LeaseToken,
			Attempt: claims.Attempt, ObservedAt: s.clock.Now().UnixNano(),
		},
	})
}

func (s *QueueService) NegativeAcknowledge(ctx context.Context, handle string, retryDelay time.Duration) (domain.Result, error) {
	if retryDelay < 0 {
		return domain.Result{}, domain.ErrInvalidCommand
	}
	claims, err := s.signer.Verify(handle)
	if err != nil {
		return domain.Result{}, err
	}
	if err := validateReceiptLineage(claims, s.group.Lineage()); err != nil {
		return domain.Result{}, err
	}
	commandID, err := s.ids.NewID()
	if err != nil {
		return domain.Result{}, err
	}
	now := s.clock.Now()
	return s.group.Propose(ctx, domain.Command{
		SchemaVersion: domain.CommandVersion,
		CommandID:     commandID,
		Lineage:       s.group.Lineage(),
		Type:          domain.CommandNegativeAck,
		NegativeAck: &domain.NegativeAck{
			MessageID: claims.MessageID, LeaseToken: claims.LeaseToken, Attempt: claims.Attempt,
			ObservedAt: now.UnixNano(), AvailableAt: now.Add(retryDelay).UnixNano(),
		},
	})
}

// RunDueTransitions is invoked only by the current partition leader. The due
// query is advisory; each proposed command carries the expected deadline and
// lease identity, making races and duplicate timer runs deterministic no-ops.
func (s *QueueService) RunDueTransitions(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, domain.ErrInvalidCommand
	}
	now := s.clock.Now().UnixNano()
	delayed, leases, err := s.group.DueTransitions(ctx, now, limit)
	if err != nil {
		return 0, err
	}
	applied := 0
	for _, message := range leases {
		commandID, err := s.ids.NewID()
		if err != nil {
			return applied, err
		}
		result, err := s.group.Propose(ctx, domain.Command{
			SchemaVersion: domain.CommandVersion,
			CommandID:     commandID,
			Lineage:       s.group.Lineage(),
			Type:          domain.CommandExpireLease,
			ExpireLease: &domain.ExpireLease{
				MessageID: message.ID, LeaseToken: message.ActiveLeaseToken,
				Attempt: message.DeliveryAttempt, ExpectedDeadline: message.LeaseDeadline,
				ObservedAt: now,
			},
		})
		if err != nil {
			return applied, err
		}
		if result.Code == domain.ResultApplied {
			applied++
		}
	}
	for _, message := range delayed {
		commandID, err := s.ids.NewID()
		if err != nil {
			return applied, err
		}
		result, err := s.group.Propose(ctx, domain.Command{
			SchemaVersion: domain.CommandVersion,
			CommandID:     commandID,
			Lineage:       s.group.Lineage(),
			Type:          domain.CommandMakeDelayedReady,
			DelayedReady: &domain.DelayedReady{
				MessageID: message.ID, ExpectedAvailableAt: message.AvailableAt, ObservedAt: now,
			},
		})
		if err != nil {
			return applied, err
		}
		if result.Code == domain.ResultApplied {
			applied++
		}
	}
	return applied, nil
}

func validateReceiptLineage(claims receipt.Claims, lineage domain.Lineage) error {
	if claims.QueueID != lineage.QueueID || claims.GenerationID != lineage.GenerationID ||
		claims.PartitionID != lineage.PartitionID {
		return ErrReceiptLineage
	}
	return nil
}
