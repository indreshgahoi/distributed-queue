package queuegroup

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/indreshgahoi/distributed-queue/internal/dataplane/application"
	"github.com/indreshgahoi/distributed-queue/internal/dataplane/domain"
	"github.com/indreshgahoi/distributed-queue/internal/dataplane/receipt"
)

var testLineage = domain.Lineage{
	QueueID:      "queue-g2",
	GenerationID: "generation-g2",
	PartitionID:  0,
	RaftGroupID:  2001,
}

type sequenceIDs struct{ next int }

func (source *sequenceIDs) NewID() (string, error) {
	source.next++
	return fmt.Sprintf("id-%d", source.next), nil
}

type mutableClock struct{ now time.Time }

func (clock *mutableClock) Now() time.Time { return clock.now }

func TestQueueLifecycleCommandsCrossThreeReplicaGroup(t *testing.T) {
	cluster := startCluster(t, domain.DefaultConfiguration(), 10)
	leader := cluster.waitForLeader(t)
	service := newService(t, leader, &mutableClock{now: time.Unix(100, 0)})

	published, err := service.Publish(context.Background(), []byte("payload"), "producer-1")
	if err != nil || published.Code != domain.ResultApplied {
		t.Fatalf("publish: result=%+v err=%v", published, err)
	}
	delivery, err := service.Receive(context.Background(), 30*time.Second)
	if err != nil || string(delivery.Payload) != "payload" || delivery.Attempt != 1 {
		t.Fatalf("receive: delivery=%+v err=%v", delivery, err)
	}
	nacked, err := service.NegativeAcknowledge(context.Background(), delivery.ReceiptHandle, 0)
	if err != nil || nacked.Code != domain.ResultApplied {
		t.Fatalf("nack: result=%+v err=%v", nacked, err)
	}
	redelivered, err := service.Receive(context.Background(), 30*time.Second)
	if err != nil || redelivered.MessageID != delivery.MessageID || redelivered.Attempt != 2 {
		t.Fatalf("redelivery: delivery=%+v err=%v", redelivered, err)
	}
	acknowledged, err := service.Acknowledge(context.Background(), redelivered.ReceiptHandle)
	if err != nil || acknowledged.Code != domain.ResultApplied {
		t.Fatalf("ack: result=%+v err=%v", acknowledged, err)
	}

	waitForApplied(t, cluster.groups, 5, 10*time.Second)
	for _, replica := range cluster.groups {
		stats, err := replica.LocalStats()
		if err != nil || stats.RetainedMessages != 0 {
			t.Fatalf("replica %d did not converge: stats=%+v err=%v", replica.ReplicaID(), stats, err)
		}
	}
}

func TestDelayedRetryAndLeaseExpiryCrossThreeReplicaGroup(t *testing.T) {
	queue := domain.DefaultConfiguration()
	queue.MaxDeliveryAttempts = 2
	cluster := startCluster(t, queue, 10)
	leader := cluster.waitForLeader(t)
	clock := &mutableClock{now: time.Unix(100, 0)}
	service := newService(t, leader, clock)

	if _, err := service.Publish(context.Background(), []byte("payload"), "producer-1"); err != nil {
		t.Fatal(err)
	}
	first, err := service.Receive(context.Background(), 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.NegativeAcknowledge(context.Background(), first.ReceiptHandle, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Receive(context.Background(), 30*time.Second); !errors.Is(err, application.ErrEmptyQueue) {
		t.Fatalf("receive before retry deadline: %v", err)
	}
	clock.now = time.Unix(111, 0)
	if count, err := service.RunDueTransitions(context.Background(), 10); err != nil || count != 1 {
		t.Fatalf("make delayed ready: count=%d err=%v", count, err)
	}
	second, err := service.Receive(context.Background(), 30*time.Second)
	if err != nil || second.Attempt != 2 {
		t.Fatalf("second delivery: delivery=%+v err=%v", second, err)
	}
	clock.now = time.Unix(142, 0)
	if count, err := service.RunDueTransitions(context.Background(), 10); err != nil || count != 1 {
		t.Fatalf("expire final lease: count=%d err=%v", count, err)
	}
	if _, err := service.Receive(context.Background(), 30*time.Second); !errors.Is(err, application.ErrEmptyQueue) {
		t.Fatalf("dead-lettered message was delivered: %v", err)
	}
	waitForApplied(t, cluster.groups, 6, 10*time.Second)
}

func TestCommittedPublishSurvivesLeaderFailureAndRetry(t *testing.T) {
	cluster := startCluster(t, domain.DefaultConfiguration(), 10)
	leader := cluster.waitForLeader(t)
	command := publishCommand("publish-ambiguous", "message-1", "payload")

	result, err := leader.Propose(context.Background(), command)
	if err != nil || result.Code != domain.ResultApplied {
		t.Fatalf("initial publish: result=%+v err=%v", result, err)
	}
	leader.Close()
	cluster.remove(leader.ReplicaID())

	newLeader := cluster.waitForLeader(t)
	retried, err := newLeader.Propose(context.Background(), command)
	if err != nil || !reflect.DeepEqual(retried, result) {
		t.Fatalf("idempotent retry: result=%+v want=%+v err=%v", retried, result, err)
	}
	stats, err := newLeader.Stats(context.Background())
	if err != nil || stats.RetainedMessages != 1 || stats.LastAppliedIndex != 2 {
		t.Fatalf("unexpected state after retry: stats=%+v err=%v", stats, err)
	}
}

func TestSnapshotAndRetainedLogRecoverThreeReplicaPartition(t *testing.T) {
	cluster := startCluster(t, domain.DefaultConfiguration(), 0)
	leader := cluster.waitForLeader(t)
	for index := 1; index <= 20; index++ {
		command := publishCommand(
			fmt.Sprintf("publish-%d", index),
			fmt.Sprintf("message-%d", index),
			fmt.Sprintf("payload-%d", index),
		)
		if _, err := leader.Propose(context.Background(), command); err != nil {
			t.Fatalf("publish %d: %v", index, err)
		}
	}
	if _, err := leader.RequestSnapshot(context.Background()); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	for index := 21; index <= 25; index++ {
		command := publishCommand(
			fmt.Sprintf("publish-%d", index),
			fmt.Sprintf("message-%d", index),
			fmt.Sprintf("payload-%d", index),
		)
		if _, err := leader.Propose(context.Background(), command); err != nil {
			t.Fatalf("suffix publish %d: %v", index, err)
		}
	}
	waitForApplied(t, cluster.groups, 25, 10*time.Second)
	cluster.close()

	restarted := restartCluster(t, cluster)
	restartedLeader := restarted.waitForLeader(t)
	stats, err := restartedLeader.Stats(context.Background())
	if err != nil || stats.LastAppliedIndex != 25 || stats.RetainedMessages != 25 {
		t.Fatalf("recovered state: stats=%+v err=%v", stats, err)
	}
}

func TestProposalFailsWhenMajorityIsUnavailable(t *testing.T) {
	cluster := startCluster(t, domain.DefaultConfiguration(), 10)
	leader := cluster.waitForLeader(t)
	for _, replica := range append([]*Group(nil), cluster.groups...) {
		if replica.ReplicaID() == leader.ReplicaID() {
			continue
		}
		replica.Close()
		cluster.remove(replica.ReplicaID())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 750*time.Millisecond)
	defer cancel()
	if _, err := leader.Propose(ctx, publishCommand("publish", "message", "payload")); err == nil {
		t.Fatal("proposal succeeded without a majority")
	}
}

func TestInvalidEnvelopeIsRejectedBeforeReplication(t *testing.T) {
	cluster := startCluster(t, domain.DefaultConfiguration(), 10)
	leader := cluster.waitForLeader(t)
	command := publishCommand("publish", "message", "payload")
	command.Lineage.GenerationID = "foreign-generation"

	if _, err := leader.Propose(context.Background(), command); !errors.Is(err, domain.ErrLineageMismatch) {
		t.Fatalf("error = %v, want lineage mismatch", err)
	}
	stats, err := leader.Stats(context.Background())
	if err != nil || stats.LastAppliedIndex != 0 {
		t.Fatalf("invalid envelope reached apply: stats=%+v err=%v", stats, err)
	}
}

func BenchmarkReplicatedQueuePublish(b *testing.B) {
	cluster := startCluster(b, domain.DefaultConfiguration(), 0)
	leader := cluster.waitForLeader(b)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		command := publishCommand(
			fmt.Sprintf("benchmark-publish-%d", index),
			fmt.Sprintf("benchmark-message-%d", index),
			"one-kibibyte-payload-"+strings.Repeat("x", 1003),
		)
		if _, err := leader.Propose(ctx, command); err != nil {
			b.Fatal(err)
		}
	}
}

type testCluster struct {
	groups    []*Group
	addresses []string
	roots     []string
	queue     domain.Configuration
	snapshots uint64
}

func startCluster(t testing.TB, queue domain.Configuration, snapshots uint64) *testCluster {
	t.Helper()
	cluster := &testCluster{queue: queue, snapshots: snapshots}
	members := make(map[uint64]string, 3)
	for replicaID := 1; replicaID <= 3; replicaID++ {
		address := freeAddress(t)
		cluster.addresses = append(cluster.addresses, address)
		cluster.roots = append(cluster.roots, filepath.Join(t.TempDir(), fmt.Sprintf("replica-%d", replicaID)))
		members[uint64(replicaID)] = address
	}
	for replicaID := 1; replicaID <= 3; replicaID++ {
		group, err := Start(Config{
			ReplicaID:      uint64(replicaID),
			Address:        cluster.addresses[replicaID-1],
			StorageRoot:    cluster.roots[replicaID-1],
			InitialMembers: members,
			Lineage:        testLineage,
			Queue:          queue,
			RequestTimeout: 5 * time.Second,
			SnapshotEvery:  snapshots,
		})
		if err != nil {
			cluster.close()
			t.Fatal(err)
		}
		cluster.groups = append(cluster.groups, group)
	}
	t.Cleanup(cluster.close)
	return cluster
}

func restartCluster(t *testing.T, previous *testCluster) *testCluster {
	t.Helper()
	cluster := &testCluster{
		addresses: previous.addresses,
		roots:     previous.roots,
		queue:     previous.queue,
		snapshots: previous.snapshots,
	}
	for replicaID := 1; replicaID <= 3; replicaID++ {
		group, err := Start(Config{
			ReplicaID:      uint64(replicaID),
			Address:        cluster.addresses[replicaID-1],
			StorageRoot:    cluster.roots[replicaID-1],
			Lineage:        testLineage,
			Queue:          cluster.queue,
			RequestTimeout: 5 * time.Second,
			SnapshotEvery:  cluster.snapshots,
		})
		if err != nil {
			cluster.close()
			t.Fatal(err)
		}
		cluster.groups = append(cluster.groups, group)
	}
	t.Cleanup(cluster.close)
	return cluster
}

func (cluster *testCluster) waitForLeader(t testing.TB) *Group {
	t.Helper()
	leader, _, err := WaitForLeader(cluster.groups, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return leader
}

func (cluster *testCluster) remove(replicaID uint64) {
	remaining := cluster.groups[:0]
	for _, group := range cluster.groups {
		if group.ReplicaID() != replicaID {
			remaining = append(remaining, group)
		}
	}
	cluster.groups = remaining
}

func (cluster *testCluster) close() {
	for _, group := range cluster.groups {
		group.Close()
	}
}

func waitForApplied(t testing.TB, groups []*Group, index uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allApplied := true
		for _, group := range groups {
			stats, err := group.LocalStats()
			if err != nil || stats.LastAppliedIndex < index {
				allApplied = false
				break
			}
		}
		if allApplied {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("replicas did not apply index %d within %s", index, timeout)
}

func newService(t testing.TB, group *Group, clock application.Clock) *application.QueueService {
	t.Helper()
	signer, err := receipt.NewSigner([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	return application.NewQueueService(group, signer, &sequenceIDs{}, clock)
}

func publishCommand(commandID, messageID, payload string) domain.Command {
	return domain.Command{
		SchemaVersion: domain.CommandVersion,
		CommandID:     commandID,
		Lineage:       testLineage,
		Type:          domain.CommandPublish,
		Publish: &domain.Publish{
			MessageID:         messageID,
			ProducerRequestID: commandID,
			Payload:           []byte(payload),
			AvailableAt:       1,
			ObservedAt:        1,
		},
	}
}

func freeAddress(t testing.TB) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}
