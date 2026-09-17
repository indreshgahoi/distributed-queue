package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/indreshgahoi/distributed-queue/internal/controlplane/adapter/memory"
	"github.com/indreshgahoi/distributed-queue/internal/controlplane/application"
)

func TestCreateQueueHTTPContract(t *testing.T) {
	handler := testHandler()
	request := httptest.NewRequest(http.MethodPost, "/v1/tenants/tenant-1/queues",
		bytes.NewBufferString(`{"name":"orders","partitionCount":4}`))
	request.Header.Set("Idempotency-Key", "request-1")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var body queueResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.TenantID != "tenant-1" || body.Name != "orders" || body.PartitionCount != 4 || body.ReplicationFactor != 3 {
		t.Fatalf("unexpected response: %+v", body)
	}
}

func TestCreateQueueRequiresIdempotencyKey(t *testing.T) {
	handler := testHandler()
	request := httptest.NewRequest(http.MethodPost, "/v1/tenants/tenant-1/queues",
		bytes.NewBufferString(`{"name":"orders","partitionCount":4}`))
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	assertErrorCode(t, response, "INVALID_QUEUE")
}

func TestCreateQueueRejectsUnknownFields(t *testing.T) {
	handler := testHandler()
	request := httptest.NewRequest(http.MethodPost, "/v1/tenants/tenant-1/queues",
		bytes.NewBufferString(`{"name":"orders","partitionCount":4,"surprise":true}`))
	request.Header.Set("Idempotency-Key", "request-1")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	assertErrorCode(t, response, "INVALID_REQUEST")
}

func testHandler() http.Handler {
	catalog := memory.NewCatalog()
	service := application.NewQueueService(catalog, &sequentialIDs{}, fixedClock{})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewHandler(service, readyStub{}, logger)
}

type readyStub struct{}

func (readyStub) Ready(context.Context) error { return nil }

type sequentialIDs struct{ next int }

func (source *sequentialIDs) NewID() (string, error) {
	source.next++
	return "id-" + string(rune('0'+source.next)), nil
}

type fixedClock struct{}

func (fixedClock) Now() time.Time { return time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC) }

func assertErrorCode(t *testing.T, response *httptest.ResponseRecorder, want string) {
	t.Helper()
	var body errorResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if body.Code != want {
		t.Fatalf("error code = %q, want %q", body.Code, want)
	}
}
