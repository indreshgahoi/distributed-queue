package etcd

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestPutIfNewerRejectsStaleProjection(t *testing.T) {
	projection, client := integrationProjection(t)
	ctx := context.Background()
	key := "/dq/test/projections/queue-1"

	active := mustJSON(t, map[string]string{"queueId": "queue-1", "state": "ACTIVE"})
	applied, err := projection.PutIfNewer(ctx, key, 2, active)
	if err != nil || !applied {
		t.Fatalf("put version 2: applied=%v err=%v", applied, err)
	}
	provisioning := mustJSON(t, map[string]string{"queueId": "queue-1", "state": "PROVISIONING"})
	applied, err = projection.PutIfNewer(ctx, key, 1, provisioning)
	if err != nil {
		t.Fatalf("put stale version: %v", err)
	}
	if applied {
		t.Fatal("stale projection was applied")
	}

	stored := loadEnvelope(t, client, key)
	if stored.Version != 2 {
		t.Fatalf("stored version = %d, want 2", stored.Version)
	}
}

func TestPutIfNewerConvergesToHighestConcurrentVersion(t *testing.T) {
	projection, client := integrationProjection(t)
	ctx := context.Background()
	key := "/dq/test/projections/queue-2"

	const highest = 12
	errorsSeen := make(chan error, highest)
	var group sync.WaitGroup
	for version := uint64(1); version <= highest; version++ {
		group.Add(1)
		go func(version uint64) {
			defer group.Done()
			payload, err := json.Marshal(map[string]uint64{"version": version})
			if err == nil {
				_, err = projection.PutIfNewer(ctx, key, version, payload)
			}
			errorsSeen <- err
		}(version)
	}
	group.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil {
			t.Fatalf("concurrent projection: %v", err)
		}
	}

	stored := loadEnvelope(t, client, key)
	if stored.Version != highest {
		t.Fatalf("stored version = %d, want %d", stored.Version, highest)
	}
}

func integrationProjection(t *testing.T) (*Projection, *clientv3.Client) {
	t.Helper()
	endpoint := os.Getenv("DQ_TEST_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("DQ_TEST_ETCD_ENDPOINT is not set")
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("open etcd client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if _, err := client.Delete(context.Background(), "/dq/test/", clientv3.WithPrefix()); err != nil {
		t.Fatalf("reset etcd test prefix: %v", err)
	}
	return NewProjection(client), client
}

func loadEnvelope(t *testing.T, client *clientv3.Client, key string) envelope {
	t.Helper()
	response, err := client.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("get projection: %v", err)
	}
	if len(response.Kvs) != 1 {
		t.Fatalf("got %d values, want 1", len(response.Kvs))
	}
	var stored envelope
	if err := json.Unmarshal(response.Kvs[0].Value, &stored); err != nil {
		t.Fatalf("decode projection: %v", err)
	}
	return stored
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("encode test payload: %v", err)
	}
	return encoded
}
