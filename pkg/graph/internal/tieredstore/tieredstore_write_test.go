package tieredstore

import (
	"errors"
	"sort"
	"testing"

	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// ─── DeleteRelationshipsBatch ─────────────────────────────────────────────────
//
// These tests cover the v3.1.0 partitioning optimisation: same-shard rels
// collapse into per-shard BadgerStore.DeleteRelationshipsBatch calls; cross-shard
// rels continue down the per-ID DeleteRelationship path.

// setupBatchDelete creates a TieredStore with Case+User as reference labels and
// returns the store plus the resolved tokens for Case (ref) and Signal (event).
func setupBatchDelete(t *testing.T) (*TieredStore, uint16, uint16) {
	t.Helper()
	ts := newTestTieredStore(t)
	reg := indexpkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")
	return ts, caseTok, signalTok
}

// TestTieredStore_DeleteRelationshipsBatch_EmptyInput verifies the no-op contract.
// nil and empty slice MUST return nil and perform no work.
func TestTieredStore_DeleteRelationshipsBatch_EmptyInput(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)

	if err := ts.DeleteRelationshipsBatch(nil); err != nil {
		t.Errorf("DeleteRelationshipsBatch(nil): %v", err)
	}
	if err := ts.DeleteRelationshipsBatch([]types.RelID{}); err != nil {
		t.Errorf("DeleteRelationshipsBatch([]): %v", err)
	}
}

// TestTieredStore_DeleteRelationshipsBatch_SameShardBucketed verifies that
// many same-shard rels delete correctly via the new partitioning path. Both
// endpoints are event nodes so all rels live entirely on the hot shard.
func TestTieredStore_DeleteRelationshipsBatch_SameShardBucketed(t *testing.T) {
	t.Parallel()
	ts, _, signalTok := setupBatchDelete(t)

	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	const N = 200
	// Two event nodes are enough endpoints to share among many rels.
	a := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	b := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	if err := ts.PutNode(a); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutNode(b); err != nil {
		t.Fatal(err)
	}

	ids := make([]types.RelID, 0, N)
	for i := 0; i < N; i++ {
		r := types.NewRelationship(types.RelID(relGen.Generate()), 1, a.ID(), b.ID())
		if err := ts.PutRelationship(r); err != nil {
			t.Fatalf("PutRelationship %d: %v", i, err)
		}
		ids = append(ids, r.ID())
	}

	// All rels live on the hot event shard.
	if got, _ := ts.RelationshipCount(); got != N {
		t.Fatalf("pre-delete RelationshipCount = %d, want %d", got, N)
	}

	if err := ts.DeleteRelationshipsBatch(ids); err != nil {
		t.Fatalf("DeleteRelationshipsBatch: %v", err)
	}

	// All rels gone.
	if got, _ := ts.RelationshipCount(); got != 0 {
		t.Errorf("post-delete RelationshipCount = %d, want 0", got)
	}
	// Adjacency cleaned: outgoing from a / incoming into b are empty.
	out, err := ts.OutgoingRelationships(a.ID(), 0)
	if err != nil {
		t.Fatalf("OutgoingRelationships: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("OutgoingRelationships(a) = %d, want 0", len(out))
	}
	in, err := ts.IncomingRelationships(b.ID(), 0)
	if err != nil {
		t.Fatalf("IncomingRelationships: %v", err)
	}
	if len(in) != 0 {
		t.Errorf("IncomingRelationships(b) = %d, want 0", len(in))
	}
	// Each ID surfaces ErrRelNotFound on lookup.
	for _, id := range ids[:5] { // spot check — full N iteration is noisy
		if _, err := ts.GetRelationship(id); !errors.Is(err, storepkg.ErrRelNotFound) {
			t.Errorf("GetRelationship(%d): err = %v, want ErrRelNotFound", id, err)
		}
	}
}

// TestTieredStore_DeleteRelationshipsBatch_CrossShardOnly verifies that an
// all-cross-shard input still deletes correctly. Cross-shard rels skip the
// per-shard batch and go through the existing per-ID DeleteRelationship path.
// Endpoints: Case (ref shard) ←→ Signal (event shard).
func TestTieredStore_DeleteRelationshipsBatch_CrossShardOnly(t *testing.T) {
	t.Parallel()
	ts, caseTok, signalTok := setupBatchDelete(t)

	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	const N = 20
	caseNode := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	signalNode := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	if err := ts.PutNode(caseNode); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutNode(signalNode); err != nil {
		t.Fatal(err)
	}

	ids := make([]types.RelID, 0, N)
	for i := 0; i < N; i++ {
		// Case (ref) -> Signal (event) = cross-shard rel.
		r := types.NewRelationship(types.RelID(relGen.Generate()), 1, caseNode.ID(), signalNode.ID())
		if err := ts.PutRelationship(r); err != nil {
			t.Fatalf("PutRelationship %d: %v", i, err)
		}
		ids = append(ids, r.ID())
	}

	if got, _ := ts.RelationshipCount(); got != N {
		t.Fatalf("pre-delete RelationshipCount = %d, want %d", got, N)
	}

	if err := ts.DeleteRelationshipsBatch(ids); err != nil {
		t.Fatalf("DeleteRelationshipsBatch (cross-shard): %v", err)
	}

	// All rels gone — entity+out/ on ref shard, in/ on event shard.
	if got, _ := ts.RelationshipCount(); got != 0 {
		t.Errorf("post-delete RelationshipCount = %d, want 0", got)
	}
	// in/ on the end-node shard (event) must be cleared. Use the test helper
	// to inspect the actual end-shard adjacency so we catch a regression where
	// the cross-shard rollback ordering breaks.
	endShard, endCheckin, err := ts.shardForNodeIDChecked(signalNode.ID())
	if err != nil {
		t.Fatal(err)
	}
	defer endCheckin()
	if leftover := endShard.IncomingRelIDs(signalNode.ID().SnowflakeID(), 0); len(leftover) != 0 {
		t.Errorf("end-shard in/ leftover after cross-shard batch delete: %d entries", len(leftover))
	}

	// RunRepair must not detect any orphaned in/ entries — proves the
	// cross-shard split-delete completed both legs for every ID.
	res, err := ts.RunRepair()
	if err != nil {
		t.Fatalf("RunRepair: %v", err)
	}
	if res.OrphanedInEntries != 0 {
		t.Errorf("RunRepair: %d orphaned in/ entries after batch delete", res.OrphanedInEntries)
	}
}

// TestTieredStore_DeleteRelationshipsBatch_Mixed verifies that an arbitrary
// interleaving of same-shard and cross-shard rels deletes correctly. This is
// the primary "behavioural parity" test from the design plan.
func TestTieredStore_DeleteRelationshipsBatch_Mixed(t *testing.T) {
	t.Parallel()
	ts, caseTok, signalTok := setupBatchDelete(t)

	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	// Endpoints: two ref nodes (Case) and two event nodes (Signal).
	c1 := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	c2 := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	s1 := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	s2 := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	for _, n := range []*types.Node{c1, c2, s1, s2} {
		if err := ts.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}

	const sameShardCount = 100
	const crossShardCount = 30

	ids := make([]types.RelID, 0, sameShardCount+crossShardCount)
	sameShardIDs := make([]types.RelID, 0, sameShardCount)
	crossShardIDs := make([]types.RelID, 0, crossShardCount)

	// Same-shard rels: Signal -> Signal (both endpoints on hot event shard).
	for i := 0; i < sameShardCount; i++ {
		r := types.NewRelationship(types.RelID(relGen.Generate()), 1, s1.ID(), s2.ID())
		if err := ts.PutRelationship(r); err != nil {
			t.Fatalf("PutRelationship same-shard %d: %v", i, err)
		}
		sameShardIDs = append(sameShardIDs, r.ID())
	}
	// Cross-shard rels: Case (ref) -> Signal (event).
	for i := 0; i < crossShardCount; i++ {
		r := types.NewRelationship(types.RelID(relGen.Generate()), 1, c1.ID(), s1.ID())
		if err := ts.PutRelationship(r); err != nil {
			t.Fatalf("PutRelationship cross-shard %d: %v", i, err)
		}
		crossShardIDs = append(crossShardIDs, r.ID())
	}

	// Interleave the two slices so neither bucket appears in any obvious order.
	maxLen := len(sameShardIDs)
	if len(crossShardIDs) > maxLen {
		maxLen = len(crossShardIDs)
	}
	for i := 0; i < maxLen; i++ {
		if i < len(sameShardIDs) {
			ids = append(ids, sameShardIDs[i])
		}
		if i < len(crossShardIDs) {
			ids = append(ids, crossShardIDs[i])
		}
	}

	totalBefore := sameShardCount + crossShardCount
	if got, _ := ts.RelationshipCount(); got != totalBefore {
		t.Fatalf("pre-delete RelationshipCount = %d, want %d", got, totalBefore)
	}

	if err := ts.DeleteRelationshipsBatch(ids); err != nil {
		t.Fatalf("DeleteRelationshipsBatch (mixed): %v", err)
	}

	if got, _ := ts.RelationshipCount(); got != 0 {
		t.Errorf("post-delete RelationshipCount = %d, want 0", got)
	}

	// Sanity: each ID is gone.
	gone := append(append([]types.RelID(nil), sameShardIDs...), crossShardIDs...)
	sort.Slice(gone, func(i, j int) bool { return gone[i] < gone[j] })
	for _, id := range []types.RelID{gone[0], gone[len(gone)/2], gone[len(gone)-1]} {
		if _, err := ts.GetRelationship(id); !errors.Is(err, storepkg.ErrRelNotFound) {
			t.Errorf("GetRelationship(%d) after batch delete: err = %v, want ErrRelNotFound", id, err)
		}
	}

	// Cross-shard repair invariant: no orphaned in/ entries on the end shard.
	res, err := ts.RunRepair()
	if err != nil {
		t.Fatalf("RunRepair: %v", err)
	}
	if res.OrphanedInEntries != 0 {
		t.Errorf("RunRepair: %d orphaned in/ entries after mixed batch delete", res.OrphanedInEntries)
	}
}

// TestTieredStore_DeleteRelationshipsBatch_MissingID verifies that a missing
// rel ID surfaces ErrRelNotFound, matching the per-ID DeleteRelationship
// contract. The previous loop body would also return ErrRelNotFound on the
// first miss; the partitioned path must do the same.
func TestTieredStore_DeleteRelationshipsBatch_MissingID(t *testing.T) {
	t.Parallel()
	ts, _, signalTok := setupBatchDelete(t)

	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	a := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	b := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	if err := ts.PutNode(a); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutNode(b); err != nil {
		t.Fatal(err)
	}
	r := types.NewRelationship(types.RelID(relGen.Generate()), 1, a.ID(), b.ID())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatal(err)
	}

	// One real ID, one phantom ID generated from the same generator (so the
	// timestamp routes to the hot shard) but never persisted.
	phantom := types.RelID(relGen.Generate())
	err := ts.DeleteRelationshipsBatch([]types.RelID{r.ID(), phantom})
	if !errors.Is(err, storepkg.ErrRelNotFound) {
		t.Fatalf("DeleteRelationshipsBatch with missing ID: err = %v, want ErrRelNotFound", err)
	}
}
