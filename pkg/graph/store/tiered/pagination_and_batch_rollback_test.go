package tiered

import (
	"errors"
	"slices"
	"strings"
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// --- stripDepth preserves Limit/After ---

// TestTieredStore_NodesByLabel_PaginationBounded verifies that Limit is honoured
// across a multi-shard Store query (stripDepth preserves Limit and After
// instead of dropping them).
func TestTieredStore_NodesByLabel_PaginationBounded(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	evtTok, _ := reg.GetOrCreate("Signal") // event label → hot shard
	gen := tieredNodeGen(t)

	// Insert 12 event nodes; IDs come from one generator, so they ascend.
	const total = 12
	ids := make([]types.NodeID, 0, total)
	for i := 0; i < total; i++ {
		n := types.NewNode(types.NodeID(gen.Generate()), evtTok, nil)
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
		ids = append(ids, n.ID())
	}

	// Pages of 5 by After cursor: exactly ids[0:5], ids[5:10], ids[10:12].
	const limit = 5
	var after types.EntityID
	for start := 0; start < total; start += limit {
		page, err := ts.NodesByLabel(evtTok, QueryOpts{Limit: limit, After: after})
		if err != nil {
			t.Fatalf("NodesByLabel page at %d: %v", start, err)
		}
		end := min(start+limit, total)
		got := make([]types.NodeID, 0, len(page))
		for _, n := range page {
			got = append(got, n.ID())
		}
		if !slices.Equal(got, ids[start:end]) {
			t.Fatalf("page at %d = %v, want %v", start, got, ids[start:end])
		}
		after = types.EntityID(got[len(got)-1])
	}
	// The cursor past the last ID yields an empty page.
	if rest, err := ts.NodesByLabel(evtTok, QueryOpts{Limit: limit, After: after}); err != nil || len(rest) != 0 {
		t.Fatalf("page after the last ID = %d nodes (err %v), want 0", len(rest), err)
	}
}

// --- PutNodesBatch rollback on hot-shard failure ---

// TestTieredStore_PutNodesBatch_RollbackOnHotShardError verifies that when the
// hot-shard write fails, ref-shard nodes written in the same batch are rolled
// back so no orphan reference entities persist. The tiered pre-check does not
// probe the hot shard for an event-label ID, so an event node already in the hot
// shard passes it and fails only in the hot shard's own PutNodesBatch — after
// the ref bucket (first in input order) is written.
func TestTieredStore_PutNodesBatch_RollbackOnHotShardError(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")  // reference
	evtTok, _ := reg.GetOrCreate("Signal") // event (hot shard)
	gen := tieredNodeGen(t)

	dupEvtNode := types.NewNode(types.NodeID(gen.Generate()), evtTok, nil)
	if err := ts.PutNode(dupEvtNode); err != nil {
		t.Fatalf("pre-insert event node: %v", err)
	}

	newRefNode := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	err := ts.PutNodesBatch([]*types.Node{newRefNode, dupEvtNode})
	if !errors.Is(err, ErrNodeExists) {
		t.Fatalf("PutNodesBatch with an event node already in the hot shard: err = %v, want ErrNodeExists", err)
	}
	if !strings.Contains(err.Error(), "rolled back") {
		t.Fatalf("PutNodesBatch err = %v, want the hot-shard failure path (writes rolled back)", err)
	}

	// The ref bucket was written before the hot bucket failed: it must be gone.
	newRefID := newRefNode.ID()
	if _, getErr := ts.RefShardForTest().GetNode(newRefID); !errors.Is(getErr, ErrNodeNotFound) {
		t.Errorf("ref node %d after hot-shard failure: GetNode err = %v, want ErrNodeNotFound (rollback did not occur)", newRefID, getErr)
	}
}
