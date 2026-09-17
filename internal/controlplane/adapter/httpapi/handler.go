package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/indreshgahoi/distributed-queue/internal/controlplane/application"
	"github.com/indreshgahoi/distributed-queue/internal/controlplane/domain"
)

const maxRequestBytes = 1 << 20

type Handler struct {
	queues    *application.QueueService
	readiness Readiness
	logger    *slog.Logger
}

type Readiness interface {
	Ready(context.Context) error
}

func NewHandler(queues *application.QueueService, readiness Readiness, logger *slog.Logger) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	handler := &Handler{queues: queues, readiness: readiness, logger: logger}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", handler.live)
	mux.HandleFunc("GET /health/ready", handler.ready)
	mux.HandleFunc("POST /v1/tenants/{tenantId}/queues", handler.createQueue)
	return requestLogging(logger, mux)
}

type createQueueRequest struct {
	Name              string `json:"name"`
	PartitionCount    uint32 `json:"partitionCount"`
	ReplicationFactor uint32 `json:"replicationFactor,omitempty"`
}

type queueResponse struct {
	TenantID                string                `json:"tenantId"`
	QueueID                 string                `json:"queueId"`
	Name                    string                `json:"name"`
	GenerationID            string                `json:"generationId"`
	Lifecycle               domain.QueueLifecycle `json:"lifecycle"`
	PartitionCount          uint32                `json:"partitionCount"`
	ReplicationFactor       uint32                `json:"replicationFactor"`
	RoutingAlgorithmVersion uint16                `json:"routingAlgorithmVersion"`
	MetadataVersion         uint64                `json:"metadataVersion"`
}

type errorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (h *Handler) createQueue(writer http.ResponseWriter, request *http.Request) {
	tenantID := strings.TrimSpace(request.PathValue("tenantId"))
	idempotencyKey := strings.TrimSpace(request.Header.Get("Idempotency-Key"))
	var body createQueueRequest
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, maxRequestBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil {
		writeError(writer, http.StatusBadRequest, "INVALID_REQUEST", "request body must be valid JSON")
		return
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		writeError(writer, http.StatusBadRequest, "INVALID_REQUEST", "request body must contain one JSON object")
		return
	}

	queue, err := h.queues.CreateQueue(request.Context(), application.CreateQueueRequest{
		TenantID:          tenantID,
		QueueName:         strings.TrimSpace(body.Name),
		IdempotencyKey:    idempotencyKey,
		PartitionCount:    body.PartitionCount,
		ReplicationFactor: body.ReplicationFactor,
	})
	if err != nil {
		h.writeCreateError(writer, request, err)
		return
	}
	writeJSON(writer, http.StatusCreated, queueResponse{
		TenantID: queue.TenantID, QueueID: queue.QueueID, Name: queue.QueueName,
		GenerationID: queue.GenerationID, Lifecycle: queue.Lifecycle,
		PartitionCount: queue.PartitionCount, ReplicationFactor: queue.ReplicationFactor,
		RoutingAlgorithmVersion: queue.RoutingAlgorithmVersion, MetadataVersion: queue.MetadataVersion,
	})
}

func (h *Handler) writeCreateError(writer http.ResponseWriter, request *http.Request, err error) {
	switch {
	case errors.Is(err, domain.ErrInvalidQueue):
		writeError(writer, http.StatusBadRequest, "INVALID_QUEUE", err.Error())
	case errors.Is(err, domain.ErrQueueExists):
		writeError(writer, http.StatusConflict, "QUEUE_EXISTS", err.Error())
	case errors.Is(err, domain.ErrIdempotencyConflict):
		writeError(writer, http.StatusConflict, "IDEMPOTENCY_CONFLICT", err.Error())
	default:
		h.logger.ErrorContext(request.Context(), "create queue failed", "error", err)
		writeError(writer, http.StatusInternalServerError, "INTERNAL", "internal server error")
	}
}

func (h *Handler) live(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]string{"status": "UP"})
}

func (h *Handler) ready(writer http.ResponseWriter, request *http.Request) {
	if h.readiness == nil || h.readiness.Ready(request.Context()) != nil {
		writeError(writer, http.StatusServiceUnavailable, "NOT_READY", "service dependencies are unavailable")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]string{"status": "UP"})
}

func writeError(writer http.ResponseWriter, status int, code, message string) {
	writeJSON(writer, status, errorResponse{Code: code, Message: message})
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

func requestLogging(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		started := time.Now()
		observed := &statusWriter{ResponseWriter: writer, status: http.StatusOK}
		next.ServeHTTP(observed, request)
		logger.InfoContext(request.Context(), "http request",
			"method", request.Method,
			"path", request.URL.Path,
			"status", observed.status,
			"duration_ms", time.Since(started).Milliseconds(),
		)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (writer *statusWriter) WriteHeader(status int) {
	writer.status = status
	writer.ResponseWriter.WriteHeader(status)
}
