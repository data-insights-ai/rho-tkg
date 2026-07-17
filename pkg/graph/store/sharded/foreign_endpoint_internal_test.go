package sharded

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/generatedcreate"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// TestPutRelationshipForeignEnd_RejectsInvalidProof exercises the security gate
// that keeps the foreign-endpoint create path an internal graph-generated-ID
// fast path: a zero-value Proof (which a caller outside pkg/graph cannot forge)
// is rejected before any write, mirroring the other generated-create doors.
func TestPutRelationshipForeignEnd_RejectsInvalidProof(t *testing.T) {
	t.Parallel()
	s, err := New(Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	// The proof gate is checked first, so a nil relationship still reaches it.
	if err := s.PutRelationshipForeignEnd(nil, generatedcreate.Proof{}); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
		t.Fatalf("invalid proof error = %v, want ErrInvalidStoreMutation", err)
	}
}
