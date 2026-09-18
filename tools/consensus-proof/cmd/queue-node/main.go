package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/indreshgahoi/distributed-queue/internal/dataplane/application"
	"github.com/indreshgahoi/distributed-queue/internal/dataplane/domain"
	"github.com/indreshgahoi/distributed-queue/internal/dataplane/receipt"
	"github.com/indreshgahoi/distributed-queue/tools/consensus-proof/queuegroup"
)

type nodeConfig struct {
	replicaID   uint64
	raftAddress string
	httpAddress string
	storageRoot string
	members     string
	bootstrap   bool
	queueID     string
	generation  string
	partitionID uint
	groupID     uint64
	signingKey  string
}

type api struct {
	group   *queuegroup.Group
	service *application.QueueService
}

func main() {
	configure := parseFlags()
	lineage := domain.Lineage{
		QueueID:      configure.queueID,
		GenerationID: configure.generation,
		PartitionID:  uint32(configure.partitionID),
		RaftGroupID:  configure.groupID,
	}
	var members map[uint64]string
	if configure.bootstrap {
		var err error
		members, err = parseMembers(configure.members)
		if err != nil {
			fail("invalid membership", err)
		}
	}
	group, err := queuegroup.Start(queuegroup.Config{
		ReplicaID:      configure.replicaID,
		Address:        configure.raftAddress,
		StorageRoot:    configure.storageRoot,
		InitialMembers: members,
		Lineage:        lineage,
		Queue:          domain.DefaultConfiguration(),
		RequestTimeout: 5 * time.Second,
		SnapshotEvery:  10_000,
	})
	if err != nil {
		fail("start replicated queue group", err)
	}
	defer group.Close()
	signer, err := receipt.NewSigner([]byte(configure.signingKey))
	if err != nil {
		fail("configure receipt signer", err)
	}
	serverAPI := &api{
		group:   group,
		service: application.NewQueueService(group, signer, application.RandomIDSource{}, application.SystemClock{}),
	}
	server := &http.Server{
		Addr:              configure.httpAddress,
		Handler:           serverAPI.routes(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	runtimeContext, stopRuntime := context.WithCancel(context.Background())
	defer stopRuntime()
	go runDueTransitions(runtimeContext, group, serverAPI.service)

	serverErrors := make(chan error, 1)
	go func() {
		slog.Info("queue node listening",
			"replicaId", configure.replicaID,
			"raftAddress", configure.raftAddress,
			"httpAddress", configure.httpAddress,
			"raftGroupId", configure.groupID,
		)
		serverErrors <- server.ListenAndServe()
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	select {
	case received := <-signals:
		slog.Info("queue node stopping", "signal", received.String())
	case serverErr := <-serverErrors:
		if !errors.Is(serverErr, http.ErrServerClosed) {
			fail("serve queue API", serverErr)
		}
	}
	stopRuntime()
	shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownContext); err != nil {
		fail("shutdown queue API", err)
	}
}

func runDueTransitions(
	ctx context.Context,
	group *queuegroup.Group,
	service *application.QueueService,
) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			leaderID, _, valid, err := group.Leader()
			if err != nil || !valid || leaderID != group.ReplicaID() {
				continue
			}
			operationContext, cancel := context.WithTimeout(ctx, 5*time.Second)
			_, transitionErr := service.RunDueTransitions(operationContext, 100)
			cancel()
			if transitionErr != nil && ctx.Err() == nil {
				slog.Debug("due-transition cycle did not complete", "error", transitionErr)
			}
		}
	}
}

func (a *api) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", a.live)
	mux.HandleFunc("GET /health/ready", a.ready)
	mux.HandleFunc("POST /v1/messages", a.publish)
	mux.HandleFunc("POST /v1/messages/receive", a.receive)
	mux.HandleFunc("POST /v1/messages/ack", a.acknowledge)
	mux.HandleFunc("POST /v1/messages/nack", a.negativeAcknowledge)
	return mux
}

func (a *api) live(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]string{"status": "UP"})
}

func (a *api) ready(writer http.ResponseWriter, _ *http.Request) {
	leaderID, term, valid, err := a.group.Leader()
	if err != nil || !valid || leaderID == 0 {
		writeError(writer, http.StatusServiceUnavailable, "raft group has no known leader")
		return
	}
	if _, err := a.group.LocalStats(); err != nil {
		writeError(writer, http.StatusServiceUnavailable, "local replica has not recovered")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]interface{}{
		"status": "READY", "replicaId": a.group.ReplicaID(), "leaderId": leaderID, "term": term,
	})
}

func (a *api) publish(writer http.ResponseWriter, request *http.Request) {
	var body struct {
		Payload           []byte `json:"payload"`
		ProducerRequestID string `json:"producerRequestId"`
	}
	if !decodeJSON(writer, request, &body) {
		return
	}
	result, err := a.service.Publish(request.Context(), body.Payload, body.ProducerRequestID)
	writeResult(writer, result, err)
}

func (a *api) receive(writer http.ResponseWriter, request *http.Request) {
	var body struct {
		VisibilityTimeoutMillis int64 `json:"visibilityTimeoutMillis"`
	}
	if !decodeJSON(writer, request, &body) {
		return
	}
	delivery, err := a.service.Receive(
		request.Context(),
		time.Duration(body.VisibilityTimeoutMillis)*time.Millisecond,
	)
	if err != nil {
		writeServiceError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, delivery)
}

func (a *api) acknowledge(writer http.ResponseWriter, request *http.Request) {
	var body struct {
		ReceiptHandle string `json:"receiptHandle"`
	}
	if !decodeJSON(writer, request, &body) {
		return
	}
	result, err := a.service.Acknowledge(request.Context(), body.ReceiptHandle)
	writeResult(writer, result, err)
}

func (a *api) negativeAcknowledge(writer http.ResponseWriter, request *http.Request) {
	var body struct {
		ReceiptHandle    string `json:"receiptHandle"`
		RetryDelayMillis int64  `json:"retryDelayMillis"`
	}
	if !decodeJSON(writer, request, &body) {
		return
	}
	result, err := a.service.NegativeAcknowledge(
		request.Context(),
		body.ReceiptHandle,
		time.Duration(body.RetryDelayMillis)*time.Millisecond,
	)
	writeResult(writer, result, err)
}

func writeResult(writer http.ResponseWriter, result domain.Result, err error) {
	if err != nil {
		writeServiceError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, result)
}

func writeServiceError(writer http.ResponseWriter, err error) {
	status := http.StatusServiceUnavailable
	if errors.Is(err, domain.ErrInvalidCommand) || errors.Is(err, receipt.ErrInvalidReceipt) {
		status = http.StatusBadRequest
	} else if errors.Is(err, application.ErrEmptyQueue) {
		status = http.StatusNoContent
	}
	if status == http.StatusNoContent {
		writer.WriteHeader(status)
		return
	}
	writeError(writer, status, err.Error())
}

func decodeJSON(writer http.ResponseWriter, request *http.Request, target interface{}) bool {
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid JSON request")
		return false
	}
	return true
}

func writeError(writer http.ResponseWriter, status int, message string) {
	writeJSON(writer, status, map[string]string{"error": message})
}

func writeJSON(writer http.ResponseWriter, status int, value interface{}) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		slog.Error("encode HTTP response", "error", err)
	}
}

func parseFlags() nodeConfig {
	var configure nodeConfig
	flag.Uint64Var(&configure.replicaID, "replica-id", 0, "stable Raft replica ID")
	flag.StringVar(&configure.raftAddress, "raft-address", "", "advertised Raft host:port")
	flag.StringVar(&configure.httpAddress, "http-address", ":8080", "HTTP listen address")
	flag.StringVar(&configure.storageRoot, "storage-root", "", "stable replica storage directory")
	flag.StringVar(&configure.members, "members", "", "comma-separated replicaID=host:port map")
	flag.BoolVar(&configure.bootstrap, "bootstrap", true, "bootstrap a new group; false reopens existing storage")
	flag.StringVar(&configure.queueID, "queue-id", "g2-queue", "queue ID")
	flag.StringVar(&configure.generation, "generation-id", "g2-generation", "queue generation ID")
	flag.UintVar(&configure.partitionID, "partition-id", 0, "partition ID")
	flag.Uint64Var(&configure.groupID, "raft-group-id", 2001, "Raft group ID")
	flag.StringVar(
		&configure.signingKey,
		"receipt-signing-key",
		"g2-local-only-receipt-key-32-bytes",
		"receipt signing key containing at least 32 bytes",
	)
	flag.Parse()
	if configure.replicaID == 0 || configure.raftAddress == "" || configure.storageRoot == "" {
		fail("parse configuration", errors.New("replica-id, raft-address, and storage-root are required"))
	}
	return configure
}

func parseMembers(value string) (map[uint64]string, error) {
	if strings.TrimSpace(value) == "" {
		return nil, errors.New("members must not be empty when bootstrapping")
	}
	members := make(map[uint64]string)
	for _, item := range strings.Split(value, ",") {
		parts := strings.SplitN(strings.TrimSpace(item), "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid member %q", item)
		}
		replicaID, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil || replicaID == 0 || parts[1] == "" {
			return nil, fmt.Errorf("invalid member %q", item)
		}
		if _, exists := members[replicaID]; exists {
			return nil, fmt.Errorf("duplicate replica ID %d", replicaID)
		}
		members[replicaID] = parts[1]
	}
	return members, nil
}

func fail(message string, err error) {
	slog.Error(message, "error", err)
	os.Exit(1)
}
