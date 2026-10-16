package tiered

import (
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// TestTieredStoreRelationshipsByTypeAndProperty_EmptyTypeReturnsNil confirms the
// query surface is present and returns cleanly when the type has no members
// (the cross-shard scan+filter fallback path). Cross-shard population is covered
// end-to-end by the graph-layer battery.
func TestTieredStoreRelationshipsByTypeAndProperty_EmptyTypeReturnsNil(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)
	got, err := ts.RelationshipsByTypeAndProperty(1, "weight", int64(5), storecontract.QueryOpts{})
	if err != nil {
		t.Fatalf("RelationshipsByTypeAndProperty: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("empty type got %d rels, want 0", len(got))
	}
	// Phantom / unindexable value returns nil, no error.
	got, err = ts.RelationshipsByTypeAndProperty(1, "weight", []int{1, 2}, storecontract.QueryOpts{})
	if err != nil {
		t.Fatalf("RelationshipsByTypeAndProperty(unindexable): %v", err)
	}
	if got != nil {
		t.Fatalf("unindexable value got %d rels, want nil", len(got))
	}
}
