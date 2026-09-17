package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
)

const SnapshotVersion uint16 = 1

type Snapshot struct {
	Version            uint16               `json:"version"`
	Lineage            Lineage              `json:"lineage"`
	LastAppliedIndex   uint64               `json:"lastAppliedIndex"`
	LastIncludedTerm   uint64               `json:"lastIncludedTerm"`
	LogicalTimeFloor   int64                `json:"logicalTimeFloor"`
	Messages           []SnapshotMessage    `json:"messages"`
	Ready              []string             `json:"ready"`
	DeadLetters        []string             `json:"deadLetters"`
	ProducerDedupeFIFO []DedupeEntry        `json:"producerDedupe"`
	CommandResults     []CommandResultEntry `json:"commandResults"`
}

type SnapshotMessage struct {
	ID               string `json:"id"`
	Payload          []byte `json:"payload"`
	Status           Status `json:"status"`
	AvailableAt      int64  `json:"availableAt"`
	NextAttempt      int    `json:"nextAttempt"`
	DeliveryAttempt  int    `json:"deliveryAttempt"`
	ActiveLeaseToken string `json:"activeLeaseToken,omitempty"`
	LeaseDeadline    int64  `json:"leaseDeadline"`
}

type DedupeEntry struct {
	RequestID string `json:"requestId"`
	MessageID string `json:"messageId"`
}

type CommandResultEntry struct {
	CommandID string `json:"commandId"`
	Digest    string `json:"digest"`
	Result    Result `json:"result"`
}

func (s *StateMachine) CaptureSnapshot(lastIncludedTerm uint64) Snapshot {
	ids := make([]string, 0, len(s.messages))
	for id := range s.messages {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	messages := make([]SnapshotMessage, 0, len(ids))
	for _, id := range ids {
		message := s.messages[id]
		messages = append(messages, SnapshotMessage{
			ID:               message.ID,
			Payload:          cloneBytes(message.Payload),
			Status:           message.Status,
			AvailableAt:      message.AvailableAt,
			NextAttempt:      message.NextAttempt,
			DeliveryAttempt:  message.DeliveryAttempt,
			ActiveLeaseToken: message.ActiveLeaseToken,
			LeaseDeadline:    message.LeaseDeadline,
		})
	}
	dedupe := make([]DedupeEntry, 0, len(s.producerDedupeFIFO))
	for _, requestID := range s.producerDedupeFIFO {
		dedupe = append(dedupe, DedupeEntry{RequestID: requestID, MessageID: s.producerDedupe[requestID]})
	}
	commandResults := make([]CommandResultEntry, 0, len(s.commandResultFIFO))
	for _, commandID := range s.commandResultFIFO {
		record := s.commandResults[commandID]
		commandResults = append(commandResults, CommandResultEntry{
			CommandID: commandID,
			Digest:    hex.EncodeToString(record.digest[:]),
			Result:    cloneResult(record.result),
		})
	}
	return Snapshot{
		Version:            SnapshotVersion,
		Lineage:            s.lineage,
		LastAppliedIndex:   s.lastAppliedIndex,
		LastIncludedTerm:   lastIncludedTerm,
		LogicalTimeFloor:   s.logicalTimeFloor,
		Messages:           messages,
		Ready:              append([]string(nil), s.ready...),
		DeadLetters:        append([]string(nil), s.deadLetters...),
		ProducerDedupeFIFO: dedupe,
		CommandResults:     commandResults,
	}
}

func (snapshot Snapshot) Marshal() ([]byte, error) {
	return json.Marshal(snapshot)
}

func RestoreSnapshot(data []byte, config Configuration, expected Lineage) (*StateMachine, uint64, error) {
	var snapshot Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return nil, 0, err
	}
	if snapshot.Version != SnapshotVersion || snapshot.Lineage != expected || !config.valid() {
		return nil, 0, ErrSnapshotCorrupt
	}
	machine, err := NewStateMachine(expected, config)
	if err != nil {
		return nil, 0, err
	}
	machine.lastAppliedIndex = snapshot.LastAppliedIndex
	machine.logicalTimeFloor = snapshot.LogicalTimeFloor
	for _, saved := range snapshot.Messages {
		if saved.ID == "" || machine.messages[saved.ID] != nil {
			return nil, 0, ErrSnapshotCorrupt
		}
		message := &Message{
			ID:               saved.ID,
			Payload:          cloneBytes(saved.Payload),
			Status:           saved.Status,
			AvailableAt:      saved.AvailableAt,
			NextAttempt:      saved.NextAttempt,
			DeliveryAttempt:  saved.DeliveryAttempt,
			ActiveLeaseToken: saved.ActiveLeaseToken,
			LeaseDeadline:    saved.LeaseDeadline,
		}
		machine.messages[message.ID] = message
		machine.retainedMessages++
		machine.retainedBytes += int64(len(message.Payload))
	}
	for _, id := range snapshot.Ready {
		message := machine.messages[id]
		if message == nil || message.Status != StatusReady {
			return nil, 0, ErrSnapshotCorrupt
		}
		machine.ready = append(machine.ready, id)
	}
	for _, id := range snapshot.DeadLetters {
		message := machine.messages[id]
		if message == nil || message.Status != StatusDeadLetter {
			return nil, 0, ErrSnapshotCorrupt
		}
		machine.deadLetters = append(machine.deadLetters, id)
	}
	if len(snapshot.ProducerDedupeFIFO) > config.MaxDeduplicationIDs {
		return nil, 0, ErrSnapshotCorrupt
	}
	for _, entry := range snapshot.ProducerDedupeFIFO {
		if entry.RequestID == "" || entry.MessageID == "" || machine.producerDedupe[entry.RequestID] != "" {
			return nil, 0, ErrSnapshotCorrupt
		}
		machine.producerDedupe[entry.RequestID] = entry.MessageID
		machine.producerDedupeFIFO = append(machine.producerDedupeFIFO, entry.RequestID)
	}
	if len(snapshot.CommandResults) > config.MaxCommandResults {
		return nil, 0, ErrSnapshotCorrupt
	}
	for _, entry := range snapshot.CommandResults {
		digestBytes, err := hex.DecodeString(entry.Digest)
		_, duplicate := machine.commandResults[entry.CommandID]
		if err != nil || len(digestBytes) != sha256.Size || entry.CommandID == "" || duplicate {
			return nil, 0, ErrSnapshotCorrupt
		}
		var digest [sha256.Size]byte
		copy(digest[:], digestBytes)
		machine.commandResults[entry.CommandID] = commandRecord{digest: digest, result: cloneResult(entry.Result)}
		machine.commandResultFIFO = append(machine.commandResultFIFO, entry.CommandID)
	}
	return machine, snapshot.LastIncludedTerm, nil
}
