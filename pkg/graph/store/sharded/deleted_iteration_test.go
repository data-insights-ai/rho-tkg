package sharded

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// var-time compile check that sharded.Store keeps satisfying the capability
// the graph layer type-asserts for — see stats_iter.go's own assertion too;
// this one lives here so a regression shows up next to the tests that would
// actually fail with it.
var _ storecontract.DeletedIterationCapability = (*Store)(nil)

// BACKLOG 20f: sharded.Store had no ForEachDeletedNodeID/ForEachDeletedRelID
// at all — storecontract.DeletedIterationCapability (unlike
// TransactionTimeQuery, HistoryRollbackTrim, label/rel-type-tx membership,
// and depth iteration, which sharded's package doc comment explicitly
// documents as intentional declines) was simply never implemented, an
// oversight rather than a design decision: sharded routes every entity to
// exactly ONE shard by ID (never by time window like tiered), so folding
// deleted-only iteration across shards needs no cross-shard dedup and is a
// direct fan-out to each shard's own (already-implemented, badger-native)
// DeletedIterationCapability. Without it, internal/core's
// forEachDeletedNodeIDByDepth/forEachDeletedRelIDByDepth silently fall back
// to O(total history) full-history iteration for every temporal adjacency
// query on a sharded deployment.
//
// This proves the capability is now present and folds correctly across
// multiple shards: nodes/rels live on different slots, some deleted (history
// row, no current row) and some still live, spread across shards.

func TestForEachDeletedNodeID_FoldsAcrossShards(t *testing.T) {
	st := newMemStore(t, 0, 4)

	var deleted, live []types.NodeID
	for slot := uint8(0); slot < 4; slot++ {
		id := mkNodeID(slot, 1)
		putNode(t, st, id, 10)
		live = append(live, id)
	}
	for slot := uint8(0); slot < 4; slot++ {
		id := mkNodeID(slot, 2)
		putNode(t, st, id, 10)
		cur, err := st.GetNode(id)
		if err != nil {
			t.Fatalf("GetNode: %v", err)
		}
		if err := st.DeleteNodeWithHistory(id, cur.Version(), cur, nil); err != nil {
			t.Fatalf("DeleteNodeWithHistory: %v", err)
		}
		deleted = append(deleted, id)
	}

	var got []types.NodeID
	if err := st.ForEachDeletedNodeID(func(id types.NodeID) bool {
		got = append(got, id)
		return true
	}); err != nil {
		t.Fatalf("ForEachDeletedNodeID: %v", err)
	}

	if len(got) != len(deleted) {
		t.Fatalf("ForEachDeletedNodeID visited %d IDs, want %d (deleted set from every shard) — BACKLOG 20f regression", len(got), len(deleted))
	}
	gotSet := map[types.NodeID]bool{}
	for _, id := range got {
		gotSet[id] = true
	}
	for _, id := range deleted {
		if !gotSet[id] {
			t.Fatalf("ForEachDeletedNodeID missed deleted node %v", id)
		}
	}
	for _, id := range live {
		if gotSet[id] {
			t.Fatalf("ForEachDeletedNodeID visited still-live node %v — must only yield entities with history but no current row", id)
		}
	}
}

func TestForEachDeletedRelID_FoldsAcrossShards(t *testing.T) {
	st := newMemStore(t, 0, 4)
	a := mkNodeID(0, 100)
	b := mkNodeID(1, 100)
	putNode(t, st, a, 10)
	putNode(t, st, b, 10)

	var deleted, live []types.RelID
	for slot := uint8(0); slot < 4; slot++ {
		id := mkRelID(slot, 1)
		putRel(t, st, id, 5, a, b)
		live = append(live, id)
	}
	for slot := uint8(0); slot < 4; slot++ {
		id := mkRelID(slot, 2)
		putRel(t, st, id, 5, a, b)
		cur, err := st.GetRelationship(id)
		if err != nil {
			t.Fatalf("GetRelationship: %v", err)
		}
		if err := st.DeleteRelWithHistory(id, cur.Version(), cur); err != nil {
			t.Fatalf("DeleteRelWithHistory: %v", err)
		}
		deleted = append(deleted, id)
	}

	var got []types.RelID
	if err := st.ForEachDeletedRelID(func(id types.RelID) bool {
		got = append(got, id)
		return true
	}); err != nil {
		t.Fatalf("ForEachDeletedRelID: %v", err)
	}

	if len(got) != len(deleted) {
		t.Fatalf("ForEachDeletedRelID visited %d IDs, want %d — BACKLOG 20f regression", len(got), len(deleted))
	}
	gotSet := map[types.RelID]bool{}
	for _, id := range got {
		gotSet[id] = true
	}
	for _, id := range deleted {
		if !gotSet[id] {
			t.Fatalf("ForEachDeletedRelID missed deleted rel %v", id)
		}
	}
	for _, id := range live {
		if gotSet[id] {
			t.Fatalf("ForEachDeletedRelID visited still-live rel %v", id)
		}
	}
}

// TestForEachDeletedNodeID_EarlyStopAndNilCallback pins the same contract
// every other ForEach* door on this store honors: fn==nil is rejected, and
// returning false from fn stops iteration early without a full-shard scan.
func TestForEachDeletedNodeID_EarlyStopAndNilCallback(t *testing.T) {
	st := newMemStore(t, 0, 2)
	for slot := uint8(0); slot < 2; slot++ {
		id := mkNodeID(slot, 1)
		putNode(t, st, id, 10)
		cur, _ := st.GetNode(id)
		if err := st.DeleteNodeWithHistory(id, cur.Version(), cur, nil); err != nil {
			t.Fatalf("DeleteNodeWithHistory: %v", err)
		}
	}

	if err := st.ForEachDeletedNodeID(nil); err == nil {
		t.Fatalf("ForEachDeletedNodeID(nil): want error, got nil")
	}

	count := 0
	if err := st.ForEachDeletedNodeID(func(types.NodeID) bool {
		count++
		return false
	}); err != nil {
		t.Fatalf("ForEachDeletedNodeID early-stop: %v", err)
	}
	if count != 1 {
		t.Fatalf("early-stop visited %d, want exactly 1 (fn returned false on first call)", count)
	}
}

// TestShardedStore_ForEachDeletedNodeID mirrors
// TestTieredStore_ForEachDeletedNodeID (store/tiered/foreach_test.go), the
// closest structural template (also a multi-shard backend), extended to span
// MULTIPLE DISTINCT shards — the dimension a single-shard/tiered-time-window
// test cannot exercise. A "deleted" (history-only, no current row) node is
// built directly via PutNodeVersion without ever calling PutNode, exactly as
// the tiered test does.
func TestShardedStore_ForEachDeletedNodeID(t *testing.T) {
	t.Parallel()
	st := newMemStore(t, 0, 4) // slots 0..3

	// Live node on slot 0, history-only (deleted) node on slot 0 too.
	live0 := putNode(t, st, mkNodeID(0, 1), 7)
	del0 := types.NewNode(mkNodeID(0, 2), 7, nil)
	if err := st.PutNodeVersion(del0.ID(), 0, del0); err != nil {
		t.Fatalf("PutNodeVersion(del0): %v", err)
	}

	// Live node on slot 2, history-only (deleted) node on slot 3 — DIFFERENT
	// shards from each other and from the slot-0 pair above.
	live2 := putNode(t, st, mkNodeID(2, 1), 7)
	del3 := types.NewNode(mkNodeID(3, 1), 7, nil)
	if err := st.PutNodeVersion(del3.ID(), 0, del3); err != nil {
		t.Fatalf("PutNodeVersion(del3): %v", err)
	}

	seen := make(map[snowflake.ID]struct{})
	if err := st.ForEachDeletedNodeID(func(id types.NodeID) bool {
		seen[id.SnowflakeID()] = struct{}{}
		return true
	}); err != nil {
		t.Fatalf("ForEachDeletedNodeID: %v", err)
	}

	// Exact-set assertion (rule 16): exactly the two deleted IDs, nothing else.
	want := map[snowflake.ID]struct{}{
		del0.ID().SnowflakeID(): {},
		del3.ID().SnowflakeID(): {},
	}
	if len(seen) != len(want) {
		t.Fatalf("ForEachDeletedNodeID visited %d ids, want %d: got=%v want=%v", len(seen), len(want), seen, want)
	}
	for id := range want {
		if _, ok := seen[id]; !ok {
			t.Errorf("deleted node %d should appear, got %v", id, seen)
		}
	}
	if _, ok := seen[live0.ID().SnowflakeID()]; ok {
		t.Errorf("live node %d (slot 0) must NOT appear", live0.ID())
	}
	if _, ok := seen[live2.ID().SnowflakeID()]; ok {
		t.Errorf("live node %d (slot 2) must NOT appear", live2.ID())
	}
}

// TestShardedStore_ForEachDeletedRelID is the rel counterpart, also spanning
// multiple shards (a relationship's OWN ID determines its owning shard,
// ADR-0007 — independent of its endpoints' shards).
func TestShardedStore_ForEachDeletedRelID(t *testing.T) {
	t.Parallel()
	st := newMemStore(t, 0, 4)

	a := putNode(t, st, mkNodeID(0, 1), 7)
	b := putNode(t, st, mkNodeID(0, 2), 7)

	// Live rel on slot 1.
	rLive := putRel(t, st, mkRelID(1, 1), 5, a.ID(), b.ID())

	// Deleted (history-only) rels on slots 1 (same shard as the live one) and
	// 2 (a different shard entirely).
	rDelSameShard := types.NewRelationship(mkRelID(1, 2), 5, a.ID(), b.ID())
	if err := st.PutRelVersion(rDelSameShard.ID(), 0, rDelSameShard); err != nil {
		t.Fatalf("PutRelVersion(rDelSameShard): %v", err)
	}
	rDelOtherShard := types.NewRelationship(mkRelID(2, 1), 5, a.ID(), b.ID())
	if err := st.PutRelVersion(rDelOtherShard.ID(), 0, rDelOtherShard); err != nil {
		t.Fatalf("PutRelVersion(rDelOtherShard): %v", err)
	}

	seen := make(map[snowflake.ID]struct{})
	if err := st.ForEachDeletedRelID(func(id types.RelID) bool {
		seen[id.SnowflakeID()] = struct{}{}
		return true
	}); err != nil {
		t.Fatalf("ForEachDeletedRelID: %v", err)
	}

	want := map[snowflake.ID]struct{}{
		rDelSameShard.ID().SnowflakeID():  {},
		rDelOtherShard.ID().SnowflakeID(): {},
	}
	if len(seen) != len(want) {
		t.Fatalf("ForEachDeletedRelID visited %d ids, want %d: got=%v want=%v", len(seen), len(want), seen, want)
	}
	for id := range want {
		if _, ok := seen[id]; !ok {
			t.Errorf("deleted rel %d should appear, got %v", id, seen)
		}
	}
	if _, ok := seen[rLive.ID().SnowflakeID()]; ok {
		t.Errorf("live rel %d must NOT appear", rLive.ID())
	}
}

// TestShardedStore_ForEachDeletedNodeID_DeterministicOrder confirms repeated
// calls yield IDs in a stable, deterministic order (ascending shard index —
// forEachID iterates s.shards in slice order, which is fixed at construction).
func TestShardedStore_ForEachDeletedNodeID_DeterministicOrder(t *testing.T) {
	t.Parallel()
	st := newMemStore(t, 0, 4)

	var ids []types.NodeID
	for slot := uint8(0); slot < 4; slot++ {
		n := types.NewNode(mkNodeID(slot, 1), 7, nil)
		if err := st.PutNodeVersion(n.ID(), 0, n); err != nil {
			t.Fatalf("PutNodeVersion(slot=%d): %v", slot, err)
		}
		ids = append(ids, n.ID())
	}

	var first, second []types.NodeID
	collect := func(dst *[]types.NodeID) func(types.NodeID) bool {
		return func(id types.NodeID) bool {
			*dst = append(*dst, id)
			return true
		}
	}
	if err := st.ForEachDeletedNodeID(collect(&first)); err != nil {
		t.Fatalf("ForEachDeletedNodeID (1st): %v", err)
	}
	if err := st.ForEachDeletedNodeID(collect(&second)); err != nil {
		t.Fatalf("ForEachDeletedNodeID (2nd): %v", err)
	}
	if len(first) != len(ids) || len(second) != len(ids) {
		t.Fatalf("visited %d/%d ids (want %d each)", len(first), len(second), len(ids))
	}
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("non-deterministic order at index %d: first=%v second=%v", i, first, second)
		}
	}
}

// TestShardedStore_ForEachDeletedNodeID_EarlyStop confirms fn returning false
// stops the fan-out (no goroutines/parallel shard iteration to race against).
func TestShardedStore_ForEachDeletedNodeID_EarlyStop(t *testing.T) {
	t.Parallel()
	st := newMemStore(t, 0, 4)
	for slot := uint8(0); slot < 4; slot++ {
		n := types.NewNode(mkNodeID(slot, 1), 7, nil)
		if err := st.PutNodeVersion(n.ID(), 0, n); err != nil {
			t.Fatalf("PutNodeVersion(slot=%d): %v", slot, err)
		}
	}
	count := 0
	if err := st.ForEachDeletedNodeID(func(types.NodeID) bool {
		count++
		return false // stop after the first
	}); err != nil {
		t.Fatalf("ForEachDeletedNodeID: %v", err)
	}
	if count != 1 {
		t.Fatalf("visited %d ids after early stop, want 1", count)
	}
}

// TestShardedStore_ForEachDeleted_NilCallback mirrors the mandatory-Store nil-
// callback contract every other ForEach* door on this store already enforces.
func TestShardedStore_ForEachDeleted_NilCallback(t *testing.T) {
	t.Parallel()
	st := newMemStore(t, 0, 2)
	if err := st.ForEachDeletedNodeID(nil); err != storecontract.ErrInvalidStoreMutation {
		t.Fatalf("ForEachDeletedNodeID(nil) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := st.ForEachDeletedRelID(nil); err != storecontract.ErrInvalidStoreMutation {
		t.Fatalf("ForEachDeletedRelID(nil) = %v, want ErrInvalidStoreMutation", err)
	}
}

// The downstream-consumer proof (the core adjacency-at-t fold actually using
// this capability end to end) lives in
// pkg/graph/internal/core/sharded_deleted_adjacency_test.go — this package
// cannot import internal/core (import cycle: internal/core already imports
// store/sharded to type-assert against it).
