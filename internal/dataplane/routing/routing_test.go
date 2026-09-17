package routing

import "testing"

func TestPartitionIsStableAndBounded(t *testing.T) {
	first, err := Partition("seed-v1", "order-42", 17)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Partition("seed-v1", "order-42", 17)
	if err != nil {
		t.Fatal(err)
	}
	if first != second || first >= 17 {
		t.Fatalf("unstable or out-of-range partition: first=%d second=%d", first, second)
	}
}

func TestPartitionRejectsZeroCount(t *testing.T) {
	if _, err := Partition("seed", "key", 0); err != ErrInvalidPartitionCount {
		t.Fatalf("expected ErrInvalidPartitionCount, got %v", err)
	}
}
