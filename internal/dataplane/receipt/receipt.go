package receipt

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
)

const Version uint16 = 1

var ErrInvalidReceipt = errors.New("invalid receipt handle")

type Claims struct {
	Version      uint16 `json:"version"`
	QueueID      string `json:"queueId"`
	GenerationID string `json:"generationId"`
	PartitionID  uint32 `json:"partitionId"`
	MessageID    string `json:"messageId"`
	Attempt      int    `json:"attempt"`
	LeaseToken   string `json:"leaseToken"`
}

type Signer struct{ key []byte }

func NewSigner(key []byte) (*Signer, error) {
	if len(key) < 32 {
		return nil, errors.New("receipt signing key must contain at least 32 bytes")
	}
	return &Signer{key: append([]byte(nil), key...)}, nil
}

func (s *Signer) Sign(claims Claims) (string, error) {
	if claims.Version != Version || claims.QueueID == "" || claims.GenerationID == "" ||
		claims.MessageID == "" || claims.Attempt <= 0 || claims.LeaseToken == "" {
		return "", ErrInvalidReceipt
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, s.key)
	mac.Write(payload)
	signature := mac.Sum(nil)
	return base64.RawURLEncoding.EncodeToString(payload) + "." +
		base64.RawURLEncoding.EncodeToString(signature), nil
}

func (s *Signer) Verify(handle string) (Claims, error) {
	parts := strings.Split(handle, ".")
	if len(parts) != 2 {
		return Claims{}, ErrInvalidReceipt
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return Claims{}, ErrInvalidReceipt
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return Claims{}, ErrInvalidReceipt
	}
	mac := hmac.New(sha256.New, s.key)
	mac.Write(payload)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return Claims{}, ErrInvalidReceipt
	}
	var claims Claims
	if err := json.Unmarshal(payload, &claims); err != nil || claims.Version != Version ||
		claims.QueueID == "" || claims.GenerationID == "" || claims.MessageID == "" ||
		claims.Attempt <= 0 || claims.LeaseToken == "" {
		return Claims{}, ErrInvalidReceipt
	}
	return claims, nil
}
