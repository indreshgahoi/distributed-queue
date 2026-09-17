package domain

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

type commandRecord struct {
	digest [sha256.Size]byte
	result Result
}

type StateMachine struct {
	lineage Lineage
	config  Configuration

	messages           map[string]*Message
	ready              []string
	deadLetters        []string
	producerDedupe     map[string]string
	producerDedupeFIFO []string
	commandResults     map[string]commandRecord
	commandResultFIFO  []string

	lastAppliedIndex uint64
	logicalTimeFloor int64
	retainedMessages int
	retainedBytes    int64
}

func NewStateMachine(lineage Lineage, config Configuration) (*StateMachine, error) {
	if !lineage.valid() || !config.valid() {
		return nil, ErrInvalidCommand
	}
	return &StateMachine{
		lineage:        lineage,
		config:         config,
		messages:       make(map[string]*Message),
		producerDedupe: make(map[string]string),
		commandResults: make(map[string]commandRecord),
	}, nil
}

func (s *StateMachine) Apply(index uint64, command Command) (Result, error) {
	if index != s.lastAppliedIndex+1 {
		return Result{}, fmt.Errorf("%w: have=%d got=%d", ErrCommandGap, s.lastAppliedIndex, index)
	}
	if command.SchemaVersion != CommandVersion {
		return Result{}, ErrUnsupportedSchema
	}
	if command.CommandID == "" {
		return Result{}, ErrInvalidCommand
	}
	if command.Lineage != s.lineage {
		return Result{}, ErrLineageMismatch
	}
	digest, err := commandDigest(command)
	if err != nil {
		return Result{}, ErrInvalidCommand
	}
	if previous, ok := s.commandResults[command.CommandID]; ok {
		s.lastAppliedIndex = index
		if previous.digest != digest {
			return Result{Code: ResultRejected}, nil
		}
		return cloneResult(previous.result), nil
	}

	result, err := s.apply(command)
	if err != nil {
		if errors.Is(err, ErrCapacity) || errors.Is(err, ErrMessageTooLarge) || errors.Is(err, ErrInvalidCommand) {
			s.lastAppliedIndex = index
			result = Result{Code: ResultRejected}
			s.rememberCommandResult(command.CommandID, digest, result)
			return result, nil
		}
		return Result{}, err
	}
	s.lastAppliedIndex = index
	s.rememberCommandResult(command.CommandID, digest, result)
	return result, nil
}

func (s *StateMachine) apply(command Command) (Result, error) {
	switch command.Type {
	case CommandPublish:
		return s.applyPublish(command.Publish)
	case CommandClaim:
		return s.applyClaim(command.Claim)
	case CommandAcknowledge:
		return s.applyAcknowledge(command.Acknowledge)
	case CommandNegativeAck:
		return s.applyNegativeAck(command.NegativeAck)
	case CommandExpireLease:
		return s.applyExpireLease(command.ExpireLease)
	case CommandMakeDelayedReady:
		return s.applyDelayedReady(command.DelayedReady)
	case CommandMoveDeadLetter:
		return s.applyDeadLetter(command.DeadLetter)
	default:
		return Result{}, ErrInvalidCommand
	}
}

func (s *StateMachine) applyPublish(command *Publish) (Result, error) {
	if command == nil || command.MessageID == "" || command.ObservedAt < 0 || command.AvailableAt < 0 {
		return Result{}, ErrInvalidCommand
	}
	s.advanceTime(command.ObservedAt)
	if prior, ok := s.producerDedupe[command.ProducerRequestID]; ok && command.ProducerRequestID != "" {
		return Result{Code: ResultDuplicate, MessageID: prior}, nil
	}
	if existing := s.messages[command.MessageID]; existing != nil {
		return Result{Code: ResultDuplicate, MessageID: existing.ID}, nil
	}
	if len(command.Payload) > s.config.MaxMessageBytes {
		return Result{}, ErrMessageTooLarge
	}
	if s.retainedMessages >= s.config.MaxRetainedMessages ||
		s.retainedBytes > s.config.MaxRetainedBytes-int64(len(command.Payload)) {
		return Result{}, ErrCapacity
	}

	status := StatusReady
	if command.AvailableAt > s.logicalTimeFloor {
		status = StatusDelayed
	}
	message := &Message{
		ID:          command.MessageID,
		Payload:     cloneBytes(command.Payload),
		Status:      status,
		AvailableAt: command.AvailableAt,
		NextAttempt: 1,
	}
	s.messages[message.ID] = message
	s.retainedMessages++
	s.retainedBytes += int64(len(message.Payload))
	if status == StatusReady {
		s.ready = append(s.ready, message.ID)
	}
	if command.ProducerRequestID != "" {
		s.rememberProducerRequest(command.ProducerRequestID, message.ID)
	}
	return Result{Code: ResultApplied, MessageID: message.ID}, nil
}

func (s *StateMachine) applyClaim(command *Claim) (Result, error) {
	if command == nil || command.MessageID == "" || command.LeaseToken == "" ||
		command.ExpectedAttempt <= 0 || command.LeaseDeadline <= command.ObservedAt {
		return Result{}, ErrInvalidCommand
	}
	s.advanceTime(command.ObservedAt)
	message := s.messages[command.MessageID]
	if message == nil {
		return Result{Code: ResultNotFound, MessageID: command.MessageID}, nil
	}
	if message.Status != StatusReady || message.NextAttempt != command.ExpectedAttempt ||
		len(s.ready) == 0 || s.ready[0] != message.ID {
		return Result{Code: ResultStale, MessageID: message.ID}, nil
	}
	s.ready = s.ready[1:]
	message.Status = StatusInFlight
	message.DeliveryAttempt = command.ExpectedAttempt
	message.ActiveLeaseToken = command.LeaseToken
	message.LeaseDeadline = command.LeaseDeadline
	return Result{
		Code:       ResultApplied,
		MessageID:  message.ID,
		Payload:    cloneBytes(message.Payload),
		Attempt:    message.DeliveryAttempt,
		LeaseToken: message.ActiveLeaseToken,
	}, nil
}

func (s *StateMachine) applyAcknowledge(command *Acknowledge) (Result, error) {
	if command == nil || command.MessageID == "" || command.LeaseToken == "" || command.Attempt <= 0 {
		return Result{}, ErrInvalidCommand
	}
	s.advanceTime(command.ObservedAt)
	message := s.messages[command.MessageID]
	if message == nil {
		return Result{Code: ResultNotFound, MessageID: command.MessageID}, nil
	}
	if !matchesLease(message, command.LeaseToken, command.Attempt) {
		return Result{Code: ResultStale, MessageID: message.ID}, nil
	}
	delete(s.messages, message.ID)
	s.retainedMessages--
	s.retainedBytes -= int64(len(message.Payload))
	return Result{Code: ResultApplied, MessageID: message.ID}, nil
}

func (s *StateMachine) applyNegativeAck(command *NegativeAck) (Result, error) {
	if command == nil || command.MessageID == "" || command.LeaseToken == "" || command.Attempt <= 0 {
		return Result{}, ErrInvalidCommand
	}
	s.advanceTime(command.ObservedAt)
	message := s.messages[command.MessageID]
	if message == nil {
		return Result{Code: ResultNotFound, MessageID: command.MessageID}, nil
	}
	if !matchesLease(message, command.LeaseToken, command.Attempt) {
		return Result{Code: ResultStale, MessageID: message.ID}, nil
	}
	if message.DeliveryAttempt >= s.config.MaxDeliveryAttempts {
		s.moveDeadLetter(message)
		return Result{Code: ResultApplied, MessageID: message.ID, Attempt: message.DeliveryAttempt}, nil
	}
	s.clearLease(message)
	message.NextAttempt = command.Attempt + 1
	message.AvailableAt = command.AvailableAt
	if command.AvailableAt > s.logicalTimeFloor {
		message.Status = StatusDelayed
	} else {
		message.Status = StatusReady
		s.ready = append(s.ready, message.ID)
	}
	return Result{Code: ResultApplied, MessageID: message.ID, Attempt: message.NextAttempt}, nil
}

func (s *StateMachine) applyExpireLease(command *ExpireLease) (Result, error) {
	if command == nil || command.MessageID == "" || command.LeaseToken == "" || command.Attempt <= 0 {
		return Result{}, ErrInvalidCommand
	}
	s.advanceTime(command.ObservedAt)
	message := s.messages[command.MessageID]
	if message == nil {
		return Result{Code: ResultNotFound, MessageID: command.MessageID}, nil
	}
	if !matchesLease(message, command.LeaseToken, command.Attempt) ||
		message.LeaseDeadline != command.ExpectedDeadline || command.ObservedAt < message.LeaseDeadline {
		return Result{Code: ResultStale, MessageID: message.ID}, nil
	}
	if message.DeliveryAttempt >= s.config.MaxDeliveryAttempts {
		s.moveDeadLetter(message)
		return Result{Code: ResultApplied, MessageID: message.ID, Attempt: message.DeliveryAttempt}, nil
	}
	s.clearLease(message)
	message.Status = StatusReady
	message.NextAttempt = command.Attempt + 1
	message.AvailableAt = command.ObservedAt
	s.ready = append(s.ready, message.ID)
	return Result{Code: ResultApplied, MessageID: message.ID, Attempt: message.NextAttempt}, nil
}

func (s *StateMachine) applyDelayedReady(command *DelayedReady) (Result, error) {
	if command == nil || command.MessageID == "" {
		return Result{}, ErrInvalidCommand
	}
	s.advanceTime(command.ObservedAt)
	message := s.messages[command.MessageID]
	if message == nil {
		return Result{Code: ResultNotFound, MessageID: command.MessageID}, nil
	}
	if message.Status != StatusDelayed || message.AvailableAt != command.ExpectedAvailableAt ||
		command.ObservedAt < message.AvailableAt {
		return Result{Code: ResultStale, MessageID: message.ID}, nil
	}
	message.Status = StatusReady
	s.ready = append(s.ready, message.ID)
	return Result{Code: ResultApplied, MessageID: message.ID, Attempt: message.NextAttempt}, nil
}

func (s *StateMachine) applyDeadLetter(command *DeadLetter) (Result, error) {
	if command == nil || command.MessageID == "" || command.ExpectedAttempt <= 0 {
		return Result{}, ErrInvalidCommand
	}
	s.advanceTime(command.ObservedAt)
	message := s.messages[command.MessageID]
	if message == nil {
		return Result{Code: ResultNotFound, MessageID: command.MessageID}, nil
	}
	if message.DeliveryAttempt != command.ExpectedAttempt {
		return Result{Code: ResultStale, MessageID: message.ID}, nil
	}
	s.moveDeadLetter(message)
	return Result{Code: ResultApplied, MessageID: message.ID, Attempt: message.DeliveryAttempt}, nil
}

func (s *StateMachine) NextReady() (Message, bool) {
	if len(s.ready) == 0 {
		return Message{}, false
	}
	message := s.messages[s.ready[0]]
	if message == nil || message.Status != StatusReady {
		return Message{}, false
	}
	copy := *message
	copy.Payload = cloneBytes(message.Payload)
	return copy, true
}

func (s *StateMachine) Message(messageID string) (Message, bool) {
	message := s.messages[messageID]
	if message == nil {
		return Message{}, false
	}
	copy := *message
	copy.Payload = cloneBytes(message.Payload)
	return copy, true
}

func (s *StateMachine) LastAppliedIndex() uint64 { return s.lastAppliedIndex }
func (s *StateMachine) RetainedMessages() int    { return s.retainedMessages }
func (s *StateMachine) RetainedBytes() int64     { return s.retainedBytes }
func (s *StateMachine) DeadLetterCount() int     { return len(s.deadLetters) }

func (s *StateMachine) DueDelayed(now int64, limit int) []Message {
	return s.dueMessages(now, limit, StatusDelayed)
}

func (s *StateMachine) DueLeases(now int64, limit int) []Message {
	return s.dueMessages(now, limit, StatusInFlight)
}

func (s *StateMachine) advanceTime(observedAt int64) {
	if observedAt > s.logicalTimeFloor {
		s.logicalTimeFloor = observedAt
	}
}

func (s *StateMachine) rememberProducerRequest(requestID, messageID string) {
	if len(s.producerDedupeFIFO) == s.config.MaxDeduplicationIDs {
		oldest := s.producerDedupeFIFO[0]
		s.producerDedupeFIFO = s.producerDedupeFIFO[1:]
		delete(s.producerDedupe, oldest)
	}
	s.producerDedupe[requestID] = messageID
	s.producerDedupeFIFO = append(s.producerDedupeFIFO, requestID)
}

func (s *StateMachine) rememberCommandResult(commandID string, digest [sha256.Size]byte, result Result) {
	if len(s.commandResultFIFO) == s.config.MaxCommandResults {
		oldest := s.commandResultFIFO[0]
		s.commandResultFIFO = s.commandResultFIFO[1:]
		delete(s.commandResults, oldest)
	}
	s.commandResults[commandID] = commandRecord{digest: digest, result: cloneResult(result)}
	s.commandResultFIFO = append(s.commandResultFIFO, commandID)
}

func (s *StateMachine) dueMessages(now int64, limit int, status Status) []Message {
	if limit <= 0 {
		return nil
	}
	result := make([]Message, 0, limit)
	for _, message := range s.messages {
		if message.Status != status {
			continue
		}
		deadline := message.AvailableAt
		if status == StatusInFlight {
			deadline = message.LeaseDeadline
		}
		if deadline > now {
			continue
		}
		copy := *message
		copy.Payload = nil
		result = append(result, copy)
	}
	// Map iteration is deliberately unordered. Stable ordering makes leader
	// command generation reproducible and testable even though only committed
	// commands are authoritative.
	sortMessagesByDeadlineAndID(result, status)
	if len(result) > limit {
		result = result[:limit]
	}
	return result
}

func sortMessagesByDeadlineAndID(messages []Message, status Status) {
	sort.Slice(messages, func(i, j int) bool {
		left, right := messages[i].AvailableAt, messages[j].AvailableAt
		if status == StatusInFlight {
			left, right = messages[i].LeaseDeadline, messages[j].LeaseDeadline
		}
		if left != right {
			return left < right
		}
		return messages[i].ID < messages[j].ID
	})
}

func commandDigest(command Command) ([sha256.Size]byte, error) {
	encoded, err := json.Marshal(command)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(encoded), nil
}

func cloneResult(result Result) Result {
	result.Payload = cloneBytes(result.Payload)
	return result
}

func (s *StateMachine) moveDeadLetter(message *Message) {
	message.ActiveLeaseToken = ""
	message.LeaseDeadline = 0
	message.Status = StatusDeadLetter
	s.deadLetters = append(s.deadLetters, message.ID)
}

func (s *StateMachine) clearLease(message *Message) {
	message.ActiveLeaseToken = ""
	message.LeaseDeadline = 0
	message.DeliveryAttempt = 0
}

func matchesLease(message *Message, token string, attempt int) bool {
	return message.Status == StatusInFlight &&
		message.ActiveLeaseToken == token &&
		message.DeliveryAttempt == attempt
}
