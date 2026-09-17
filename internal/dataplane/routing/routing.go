package routing

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
)

var ErrInvalidPartitionCount = errors.New("partition count must be positive")

func Partition(seed, key string, partitionCount uint32) (uint32, error) {
	if partitionCount == 0 {
		return 0, ErrInvalidPartitionCount
	}
	hash := sha256.New()
	hash.Write([]byte(seed))
	hash.Write([]byte{0})
	hash.Write([]byte(key))
	digest := hash.Sum(nil)
	return uint32(binary.BigEndian.Uint64(digest[:8]) % uint64(partitionCount)), nil
}

func Keyed(seed, messageGroupID string, partitionCount uint32) (uint32, error) {
	return Partition(seed, messageGroupID, partitionCount)
}

func Unkeyed(seed, producerRequestID string, partitionCount uint32) (uint32, error) {
	return Partition(seed, producerRequestID, partitionCount)
}
