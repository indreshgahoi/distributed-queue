package domain

import (
	"bytes"
	"errors"
	"testing"
)

var testLineage = Lineage{
	QueueID:      "queue-1",
	GenerationID: "generation-1",
	PartitionID:  0,
	RaftGroupID:  101,
}

func TestPublishClaimAndAcknowledge(t *testing.T) {
	machine := newTestMachine(t)

	published := applyTest(t, machine, 1, publishCommand("publish-1", "message-1", "request-1", "payload", 10))
	if published.Code != ResultApplied || published.MessageID != "message-1" {
		t.Fatalf("unexpected publish result: %+v", published)
	}

	claimed := applyTest(t, machine, 2, claimCommand("claim-1", "message-1", 1, "lease-1", 10, 40))
	if claimed.Code != ResultApplied || claimed.Attempt != 1 || string(claimed.Payload) != "payload" {
		t.Fatalf("unexpected claim result: %+v", claimed)
	}

	acknowledged := applyTest(t, machine, 3, Command{
		SchemaVersion: CommandVersion,
		CommandID:     "ack-1",
		Lineage:       testLineage,
		Type:          CommandAcknowledge,
		Acknowledge: &Acknowledge{
			MessageID: "message-1", LeaseToken: "lease-1", Attempt: 1, ObservedAt: 20,
		},
	})
	if acknowledged.Code != ResultApplied || machine.RetainedMessages() != 0 || machine.RetainedBytes() != 0 {
		t.Fatalf("ack did not delete the message: result=%+v count=%d bytes=%d", acknowledged, machine.RetainedMessages(), machine.RetainedBytes())
	}
}

func TestStaleAcknowledgementCannotDeleteNewDelivery(t *testing.T) {
	machine := newTestMachine(t)
	applyTest(t, machine, 1, publishCommand("publish", "message", "request", "payload", 1))
	applyTest(t, machine, 2, claimCommand("claim-1", "message", 1, "lease-1", 1, 10))
	applyTest(t, machine, 3, Command{
		SchemaVersion: CommandVersion,
		CommandID:     "expire-1",
		Lineage:       testLineage,
		Type:          CommandExpireLease,
		ExpireLease: &ExpireLease{
			MessageID: "message", LeaseToken: "lease-1", Attempt: 1, ExpectedDeadline: 10, ObservedAt: 10,
		},
	})
	applyTest(t, machine, 4, claimCommand("claim-2", "message", 2, "lease-2", 11, 30))

	stale := applyTest(t, machine, 5, Command{
		SchemaVersion: CommandVersion,
		CommandID:     "stale-ack",
		Lineage:       testLineage,
		Type:          CommandAcknowledge,
		Acknowledge: &Acknowledge{
			MessageID: "message", LeaseToken: "lease-1", Attempt: 1, ObservedAt: 12,
		},
	})
	if stale.Code != ResultStale {
		t.Fatalf("expected stale result, got %+v", stale)
	}
	message, ok := machine.Message("message")
	if !ok || message.Status != StatusInFlight || message.ActiveLeaseToken != "lease-2" {
		t.Fatalf("new delivery was changed by stale ack: %+v", message)
	}
}

func TestNegativeAckDelayAndDeadLetter(t *testing.T) {
	config := DefaultConfiguration()
	config.MaxDeliveryAttempts = 2
	machine, err := NewStateMachine(testLineage, config)
	if err != nil {
		t.Fatal(err)
	}
	applyTest(t, machine, 1, publishCommand("publish", "message", "request", "payload", 1))
	applyTest(t, machine, 2, claimCommand("claim-1", "message", 1, "lease-1", 1, 10))
	delayed := applyTest(t, machine, 3, Command{
		SchemaVersion: CommandVersion,
		CommandID:     "nack-1",
		Lineage:       testLineage,
		Type:          CommandNegativeAck,
		NegativeAck: &NegativeAck{
			MessageID: "message", LeaseToken: "lease-1", Attempt: 1, AvailableAt: 20, ObservedAt: 2,
		},
	})
	if delayed.Code != ResultApplied || delayed.Attempt != 2 {
		t.Fatalf("unexpected nack result: %+v", delayed)
	}
	stale := applyTest(t, machine, 4, Command{
		SchemaVersion: CommandVersion,
		CommandID:     "early-ready",
		Lineage:       testLineage,
		Type:          CommandMakeDelayedReady,
		DelayedReady:  &DelayedReady{MessageID: "message", ExpectedAvailableAt: 20, ObservedAt: 19},
	})
	if stale.Code != ResultStale {
		t.Fatalf("message became ready before delay: %+v", stale)
	}
	applyTest(t, machine, 5, Command{
		SchemaVersion: CommandVersion,
		CommandID:     "ready",
		Lineage:       testLineage,
		Type:          CommandMakeDelayedReady,
		DelayedReady:  &DelayedReady{MessageID: "message", ExpectedAvailableAt: 20, ObservedAt: 20},
	})
	applyTest(t, machine, 6, claimCommand("claim-2", "message", 2, "lease-2", 20, 30))
	dead := applyTest(t, machine, 7, Command{
		SchemaVersion: CommandVersion,
		CommandID:     "nack-2",
		Lineage:       testLineage,
		Type:          CommandNegativeAck,
		NegativeAck: &NegativeAck{
			MessageID: "message", LeaseToken: "lease-2", Attempt: 2, AvailableAt: 40, ObservedAt: 21,
		},
	})
	if dead.Code != ResultApplied || dead.Attempt != 2 || machine.DeadLetterCount() != 1 {
		t.Fatalf("message was not dead-lettered: result=%+v count=%d", dead, machine.DeadLetterCount())
	}
}

func TestProducerRequestIsDeduplicatedAfterAck(t *testing.T) {
	machine := newTestMachine(t)
	applyTest(t, machine, 1, publishCommand("publish-1", "message-1", "request-1", "payload", 1))
	applyTest(t, machine, 2, claimCommand("claim", "message-1", 1, "lease", 1, 10))
	applyTest(t, machine, 3, Command{
		SchemaVersion: CommandVersion,
		CommandID:     "ack",
		Lineage:       testLineage,
		Type:          CommandAcknowledge,
		Acknowledge:   &Acknowledge{MessageID: "message-1", LeaseToken: "lease", Attempt: 1, ObservedAt: 2},
	})
	duplicate := applyTest(t, machine, 4, publishCommand("publish-2", "message-2", "request-1", "different", 3))
	if duplicate.Code != ResultDuplicate || duplicate.MessageID != "message-1" || machine.RetainedMessages() != 0 {
		t.Fatalf("unexpected dedupe result: %+v retained=%d", duplicate, machine.RetainedMessages())
	}
}

func TestSnapshotRoundTripIsCanonical(t *testing.T) {
	machine := newTestMachine(t)
	applyTest(t, machine, 1, publishCommand("publish-a", "a", "request-a", "A", 1))
	applyTest(t, machine, 2, publishCommand("publish-b", "b", "request-b", "B", 1))
	applyTest(t, machine, 3, claimCommand("claim-a", "a", 1, "lease-a", 1, 10))

	first, err := machine.CaptureSnapshot(7).Marshal()
	if err != nil {
		t.Fatal(err)
	}
	restored, term, err := RestoreSnapshot(first, DefaultConfiguration(), testLineage)
	if err != nil {
		t.Fatal(err)
	}
	second, err := restored.CaptureSnapshot(term).Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("snapshot is not canonical\nfirst:  %s\nsecond: %s", first, second)
	}
	if restored.LastAppliedIndex() != 3 || term != 7 {
		t.Fatalf("logical snapshot boundary was not restored: index=%d term=%d", restored.LastAppliedIndex(), term)
	}
}

func TestApplyRejectsLogGapWithoutAdvancingState(t *testing.T) {
	machine := newTestMachine(t)
	_, err := machine.Apply(2, publishCommand("publish", "message", "request", "payload", 1))
	if !errors.Is(err, ErrCommandGap) || machine.LastAppliedIndex() != 0 || machine.RetainedMessages() != 0 {
		t.Fatalf("gap handling changed state: err=%v index=%d count=%d", err, machine.LastAppliedIndex(), machine.RetainedMessages())
	}
}

func TestCommittedRejectionAndDuplicateBothAdvanceAppliedIndex(t *testing.T) {
	machine := newTestMachine(t)
	command := publishCommand("same-command", "message", "request", "payload", 1)
	first := applyTest(t, machine, 1, command)
	duplicate := applyTest(t, machine, 2, command)
	if duplicate.Code != first.Code || duplicate.MessageID != first.MessageID ||
		!bytes.Equal(duplicate.Payload, first.Payload) || machine.LastAppliedIndex() != 2 || machine.RetainedMessages() != 1 {
		t.Fatalf("duplicate command was not replay-safe: first=%+v duplicate=%+v index=%d count=%d", first, duplicate, machine.LastAppliedIndex(), machine.RetainedMessages())
	}

	conflict := command
	conflict.Publish = &Publish{
		MessageID: "other", ProducerRequestID: "other", Payload: []byte("other"), AvailableAt: 2, ObservedAt: 2,
	}
	rejected := applyTest(t, machine, 3, conflict)
	if rejected.Code != ResultRejected || machine.LastAppliedIndex() != 3 || machine.RetainedMessages() != 1 {
		t.Fatalf("conflicting command id did not advance deterministically: result=%+v index=%d count=%d", rejected, machine.LastAppliedIndex(), machine.RetainedMessages())
	}
}

func newTestMachine(t *testing.T) *StateMachine {
	t.Helper()
	machine, err := NewStateMachine(testLineage, DefaultConfiguration())
	if err != nil {
		t.Fatal(err)
	}
	return machine
}

func applyTest(t *testing.T, machine *StateMachine, index uint64, command Command) Result {
	t.Helper()
	result, err := machine.Apply(index, command)
	if err != nil {
		t.Fatalf("apply at index %d failed: %v", index, err)
	}
	return result
}

func publishCommand(commandID, messageID, requestID, payload string, observedAt int64) Command {
	return Command{
		SchemaVersion: CommandVersion,
		CommandID:     commandID,
		Lineage:       testLineage,
		Type:          CommandPublish,
		Publish: &Publish{
			MessageID: messageID, ProducerRequestID: requestID, Payload: []byte(payload),
			AvailableAt: observedAt, ObservedAt: observedAt,
		},
	}
}

func claimCommand(commandID, messageID string, attempt int, token string, observedAt, deadline int64) Command {
	return Command{
		SchemaVersion: CommandVersion,
		CommandID:     commandID,
		Lineage:       testLineage,
		Type:          CommandClaim,
		Claim: &Claim{
			MessageID: messageID, ExpectedAttempt: attempt, LeaseToken: token,
			ObservedAt: observedAt, LeaseDeadline: deadline,
		},
	}
}
