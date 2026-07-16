package sharded

import (
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// putNodeWithWindow puts a node carrying a valid-time window on the given slot.
func putNodeWithWindow(t *testing.T, st *Store, slot uint8, n int64, label uint16, from, to types.Instant) types.NodeID {
	t.Helper()
	id := mkNodeID(slot, n)
	node := types.NewNode(id, label, nil)
	node.SetTemporal(&types.TemporalMetadata{ValidFrom: from, ValidTo: to})
	if err := st.PutNode(node); err != nil {
		t.Fatalf("PutNode(slot=%d,n=%d): %v", slot, n, err)
	}
	return id
}

// TestShardedPruneTemporalCandidatesRoutesAcrossShards proves the B4 prune routes
// each candidate id to its OWNING shard's valid-time envelope (S5 parity). Four
// nodes are placed on four distinct slots/shards with diverging windows; the prune
// at t=5000 must drop the three provably-non-overlapping nodes (bounded-past on
// slots 0 & 2, future-from on slot 3) and keep only the open node on slot 1 —
// exercising the multi-bucket byShard grouping a single-shard test cannot.
func TestShardedPruneTemporalCandidatesRoutesAcrossShards(t *testing.T) {
	t.Parallel()
	st := newMemStore(t, 0, 4) // slots 0..3
	const label = uint16(7)

	past0 := putNodeWithWindow(t, st, 0, 1, label, 1000, 2000) // bounded [1000,2000) → prune@5000
	openN := putNodeWithWindow(t, st, 1, 1, label, 1000, 0)    // open [1000,∞)     → keep@5000
	past2 := putNodeWithWindow(t, st, 2, 1, label, 1000, 2000) // bounded [1000,2000) → prune@5000
	future := putNodeWithWindow(t, st, 3, 1, label, 6000, 0)   // future [6000,∞)   → prune@5000

	// Build the temporal index AFTER the puts so every shard backfills its local
	// node's envelope.
	if err := st.CreateTemporalIndex(label); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}

	all := []types.NodeID{past0, openN, past2, future}
	kept, ok := st.PruneTemporalCandidates(label, all, storecontract.QueryOpts{ValidAt: 5000})
	if !ok {
		t.Fatal("PruneTemporalCandidates returned ok=false with indexes on every shard")
	}
	keptSet := make(map[types.NodeID]bool, len(kept))
	for _, id := range kept {
		keptSet[id] = true
	}
	if !keptSet[openN] {
		t.Errorf("open-window node on slot 1 was wrongly pruned; kept=%v", kept)
	}
	for name, id := range map[string]types.NodeID{"past0(slot0)": past0, "past2(slot2)": past2, "future(slot3)": future} {
		if keptSet[id] {
			t.Errorf("%s should have been pruned at t=5000 but survived; kept=%v", name, kept)
		}
	}
	if len(kept) != 1 {
		t.Errorf("kept %d candidates, want exactly 1 (the open node)", len(kept))
	}

	// No valid-time filter → ok=false, candidates unchanged (contract).
	if got, ok := st.PruneTemporalCandidates(label, all, storecontract.QueryOpts{}); ok || len(got) != len(all) {
		t.Errorf("no-filter prune: got (len=%d, ok=%v), want (len=%d, false)", len(got), ok, len(all))
	}

	// No temporal index for an unindexed label → ok=false (no shard can prune).
	if _, ok := st.PruneTemporalCandidates(label+1, all, storecontract.QueryOpts{ValidAt: 5000}); ok {
		t.Error("prune on an unindexed label returned ok=true; want false")
	}
}
