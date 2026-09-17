package application

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/indreshgahoi/distributed-queue/internal/dataplane/consensus/local"
	"github.com/indreshgahoi/distributed-queue/internal/dataplane/domain"
	"github.com/indreshgahoi/distributed-queue/internal/dataplane/receipt"
)

type sequenceIDs struct{ next int }

func (source *sequenceIDs) NewID() (string, error) {
	source.next++
	return fmt.Sprintf("id-%d", source.next), nil
}

type fixedClock struct{ value time.Time }

func (clock fixedClock) Now() time.Time { return clock.value }

type mutableClock struct{ value time.Time }

func (clock *mutableClock) Now() time.Time { return clock.value }

func TestPublishReceiveAndAcknowledgeCrossesConsensusBoundary(t *testing.T) {
	lineage := domain.Lineage{
		QueueID: "queue", GenerationID: "generation", PartitionID: 2, RaftGroupID: 1002,
	}
	machine, err := domain.NewStateMachine(lineage, domain.DefaultConfiguration())
	if err != nil {
		t.Fatal(err)
	}
	signer, err := receipt.NewSigner([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	service := NewQueueService(
		local.NewGroup(lineage, machine), signer, &sequenceIDs{},
		fixedClock{value: time.Unix(100, 0)},
	)

	published, err := service.Publish(context.Background(), []byte("payload"), "producer-request")
	if err != nil || published.Code != domain.ResultApplied {
		t.Fatalf("publish failed: result=%+v err=%v", published, err)
	}
	delivery, err := service.Receive(context.Background(), 30*time.Second)
	if err != nil || string(delivery.Payload) != "payload" || delivery.Attempt != 1 {
		t.Fatalf("receive failed: delivery=%+v err=%v", delivery, err)
	}
	acknowledged, err := service.Acknowledge(context.Background(), delivery.ReceiptHandle)
	if err != nil || acknowledged.Code != domain.ResultApplied || machine.RetainedMessages() != 0 {
		t.Fatalf("ack failed: result=%+v err=%v retained=%d", acknowledged, err, machine.RetainedMessages())
	}
	if machine.LastAppliedIndex() != 3 {
		t.Fatalf("operations did not cross ordered apply boundary: index=%d", machine.LastAppliedIndex())
	}
}

func TestLeaderTimerCommitsLeaseExpiryBeforeRedelivery(t *testing.T) {
	lineage := domain.Lineage{
		QueueID: "queue", GenerationID: "generation", PartitionID: 2, RaftGroupID: 1002,
	}
	machine, err := domain.NewStateMachine(lineage, domain.DefaultConfiguration())
	if err != nil {
		t.Fatal(err)
	}
	signer, err := receipt.NewSigner([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	clock := &mutableClock{value: time.Unix(100, 0)}
	service := NewQueueService(local.NewGroup(lineage, machine), signer, &sequenceIDs{}, clock)
	if _, err := service.Publish(context.Background(), []byte("payload"), "producer-request"); err != nil {
		t.Fatal(err)
	}
	first, err := service.Receive(context.Background(), 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	clock.value = time.Unix(131, 0)
	count, err := service.RunDueTransitions(context.Background(), 10)
	if err != nil || count != 1 {
		t.Fatalf("expiry was not committed: count=%d err=%v", count, err)
	}
	second, err := service.Receive(context.Background(), 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if second.MessageID != first.MessageID || second.Attempt != 2 || second.ReceiptHandle == first.ReceiptHandle {
		t.Fatalf("unexpected redelivery: first=%+v second=%+v", first, second)
	}
	stale, err := service.Acknowledge(context.Background(), first.ReceiptHandle)
	if err != nil || stale.Code != domain.ResultStale {
		t.Fatalf("old receipt was not fenced: result=%+v err=%v", stale, err)
	}
}
