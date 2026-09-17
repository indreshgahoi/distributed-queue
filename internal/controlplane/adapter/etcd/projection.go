package etcd

import (
	"context"
	"encoding/json"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const maxCASAttempts = 16

type Projection struct{ client *clientv3.Client }

func NewProjection(client *clientv3.Client) *Projection { return &Projection{client: client} }

type envelope struct {
	Version uint64          `json:"version"`
	Payload json.RawMessage `json:"payload"`
}

func (p *Projection) PutIfNewer(ctx context.Context, key string, version uint64, payload []byte) (bool, error) {
	if key == "" || version == 0 || !json.Valid(payload) {
		return false, fmt.Errorf("invalid projection update")
	}
	encoded, err := json.Marshal(envelope{Version: version, Payload: json.RawMessage(payload)})
	if err != nil {
		return false, err
	}
	for attempt := 0; attempt < maxCASAttempts; attempt++ {
		response, err := p.client.Get(ctx, key)
		if err != nil {
			return false, err
		}
		var comparison clientv3.Cmp
		if len(response.Kvs) == 0 {
			comparison = clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
		} else {
			var current envelope
			if err := json.Unmarshal(response.Kvs[0].Value, &current); err != nil || current.Version == 0 {
				return false, fmt.Errorf("invalid stored projection envelope at %s", key)
			}
			if current.Version >= version {
				return false, nil
			}
			comparison = clientv3.Compare(clientv3.ModRevision(key), "=", response.Kvs[0].ModRevision)
		}
		transaction, err := p.client.Txn(ctx).
			If(comparison).
			Then(clientv3.OpPut(key, string(encoded))).
			Commit()
		if err != nil {
			return false, err
		}
		if transaction.Succeeded {
			return true, nil
		}
	}
	return false, fmt.Errorf("projection compare-and-swap contention exceeded %d attempts", maxCASAttempts)
}
