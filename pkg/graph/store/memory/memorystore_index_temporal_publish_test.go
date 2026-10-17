package memory

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// A ReplaceNode after Phase 3 published the index keeps the old interval:
// over-inclusion by design (Extend unions, temporal_index.go:132; node removal
// is a no-op, temporal_index.go:405). This is the interleaving that made
// TestCreateTemporalIndex_ConcurrentMutationDuringScanIsReconciled flake in CI
// (run 39035363652, todo row 72) while its writes were not confined to Phase 2.
func TestCreateTemporalIndex_ReplaceAfterPublishKeepsEnvelope(t *testing.T) {
	ms := New()
	nd := memNode(1, 10)
	nd.SetTemporal(&types.TemporalMetadata{ValidFrom: types.Instant(1000)})
	if err := ms.PutNode(nd); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := ms.CreateTemporalIndex(10); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}
	updated := memNode(1, 10)
	updated.SetTemporal(&types.TemporalMetadata{ValidFrom: types.Instant(5000)})
	if err := ms.ReplaceNode(updated); err != nil {
		t.Fatalf("ReplaceNode: %v", err)
	}
	ms.mu.RLock()
	from, _, ok := ms.temporalIndexes[10].EnvelopeOf(snowflake.ID(1))
	ms.mu.RUnlock()
	if !ok || from != types.Instant(1000) {
		t.Fatalf("envelope after post-publish replace = (from=%d, ok=%v), want (1000, true)", from, ok)
	}
}
