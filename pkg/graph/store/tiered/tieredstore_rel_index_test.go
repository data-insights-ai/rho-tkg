package tiered

import (
	"errors"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// The tiered store DECLINES relationship property index creation (K3b) because
// relationships are routed to event shards by timestamp, so a shard-local
// rel-value index cannot answer a query whose matches are scattered across
// shards. Create returns the clear ErrRelPropertyIndexUnsupported sentinel; Drop
// returns ErrIndexNotFound (there is never such an index to drop).

func TestTieredStoreRelPropertyIndex_Declines(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)

	if err := ts.CreateRelPropertyIndex(1, "weight"); !errors.Is(err, storecontract.ErrRelPropertyIndexUnsupported) {
		t.Fatalf("CreateRelPropertyIndex = %v, want ErrRelPropertyIndexUnsupported", err)
	}
	if err := ts.DropRelPropertyIndex(1, "weight"); !errors.Is(err, storecontract.ErrIndexNotFound) {
		t.Fatalf("DropRelPropertyIndex = %v, want ErrIndexNotFound", err)
	}

	// Input validation still runs before the decline.
	if err := ts.CreateRelPropertyIndex(0, "weight"); err == nil {
		t.Fatal("CreateRelPropertyIndex(token 0) must reject the invalid token")
	}
	if err := ts.CreateRelPropertyIndex(1, ""); err == nil {
		t.Fatal("CreateRelPropertyIndex(empty key) must reject the invalid key")
	}
}

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
