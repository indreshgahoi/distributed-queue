package receipt

import "testing"

func TestReceiptRoundTripAndTamperDetection(t *testing.T) {
	signer, err := NewSigner([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	want := Claims{
		Version: Version, QueueID: "queue", GenerationID: "generation", PartitionID: 3,
		MessageID: "message", Attempt: 2, LeaseToken: "lease",
	}
	handle, err := signer.Sign(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := signer.Verify(handle)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("receipt changed: got=%+v want=%+v", got, want)
	}

	tampered := handle[:len(handle)-1] + "A"
	if _, err := signer.Verify(tampered); err != ErrInvalidReceipt {
		t.Fatalf("tampered receipt accepted: %v", err)
	}
}
