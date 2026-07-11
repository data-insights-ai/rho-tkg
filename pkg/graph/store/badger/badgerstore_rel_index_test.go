package badger

import (
	"errors"
	"sync"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// putRelWithWeight stores a KNOWS-typed (token 1) relationship with a weight
// property between two existing nodes.
func putRelWithWeight(t *testing.T, bs *Store, relID, startID, endID int64, typeToken uint16, weight int64) *types.Relationship {
	t.Helper()
	r := types.NewRelationship(types.RelID(snowflake.ID(relID)), typeToken, types.NodeID(snowflake.ID(startID)), types.NodeID(snowflake.ID(endID)))
	if err := r.SetProperty("weight", weight); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	if err := bs.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship(%d): %v", relID, err)
	}
	return r
}

func TestBadgerStoreCreateRelPropertyIndex_Backfill(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	putTestNode(t, bs, 1, 1, nil)
	putTestNode(t, bs, 2, 1, nil)

	putRelWithWeight(t, bs, 10, 1, 2, 1, 5)
	putRelWithWeight(t, bs, 20, 1, 2, 1, 5)
	putRelWithWeight(t, bs, 30, 1, 2, 1, 9)

	if err := bs.CreateRelPropertyIndex(1, "weight"); err != nil {
		t.Fatalf("CreateRelPropertyIndex: %v", err)
	}
	// Duplicate → ErrIndexExists.
	if err := bs.CreateRelPropertyIndex(1, "weight"); !errors.Is(err, ErrIndexExists) {
		t.Fatalf("duplicate CreateRelPropertyIndex = %v, want ErrIndexExists", err)
	}

	got, err := bs.RelationshipsByTypeAndProperty(1, "weight", int64(5), QueryOpts{})
	if err != nil {
		t.Fatalf("RelationshipsByTypeAndProperty: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("backfill weight=5: got %d rels, want 2", len(got))
	}
}

func TestBadgerStoreDropRelPropertyIndex(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	if err := bs.CreateRelPropertyIndex(1, "weight"); err != nil {
		t.Fatalf("CreateRelPropertyIndex: %v", err)
	}
	if err := bs.DropRelPropertyIndex(1, "weight"); err != nil {
		t.Fatalf("DropRelPropertyIndex: %v", err)
	}
	if err := bs.DropRelPropertyIndex(1, "weight"); !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("double DropRelPropertyIndex = %v, want ErrIndexNotFound", err)
	}
}

func TestBadgerStoreRelPropertyIndex_AutoUpdateOnReplaceAndDelete(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	putTestNode(t, bs, 1, 1, nil)
	putTestNode(t, bs, 2, 1, nil)
	r := putRelWithWeight(t, bs, 10, 1, 2, 1, 5)

	if err := bs.CreateRelPropertyIndex(1, "weight"); err != nil {
		t.Fatalf("CreateRelPropertyIndex: %v", err)
	}

	// Replace with a new weight — old value must leave, new value must enter.
	r2 := r.DeepCopy()
	_ = r2.SetProperty("weight", int64(7))
	if err := bs.ReplaceRelationship(r2); err != nil {
		t.Fatalf("ReplaceRelationship: %v", err)
	}
	if got, _ := bs.RelationshipsByTypeAndProperty(1, "weight", int64(5), QueryOpts{}); len(got) != 0 {
		t.Fatalf("after replace, old weight=5 still indexed: %d", len(got))
	}
	if got, _ := bs.RelationshipsByTypeAndProperty(1, "weight", int64(7), QueryOpts{}); len(got) != 1 {
		t.Fatalf("after replace, new weight=7 not indexed: %d", len(got))
	}

	// Delete — index entry must vanish.
	if err := bs.DeleteRelationship(types.RelID(snowflake.ID(10))); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}
	if got, _ := bs.RelationshipsByTypeAndProperty(1, "weight", int64(7), QueryOpts{}); len(got) != 0 {
		t.Fatalf("after delete, weight=7 still indexed: %d", len(got))
	}
}

func TestBadgerStoreRelPropertyIndex_SurvivesRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	bs1, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	putTestNode(t, bs1, 1, 1, nil)
	putTestNode(t, bs1, 2, 1, nil)
	putRelWithWeight(t, bs1, 10, 1, 2, 1, 5)
	putRelWithWeight(t, bs1, 20, 1, 2, 1, 5)
	putRelWithWeight(t, bs1, 30, 1, 2, 1, 9)
	if err := bs1.CreateRelPropertyIndex(1, "weight"); err != nil {
		t.Fatalf("CreateRelPropertyIndex: %v", err)
	}
	if got, _ := bs1.RelationshipsByTypeAndProperty(1, "weight", int64(5), QueryOpts{}); len(got) != 2 {
		t.Fatalf("before close: weight=5 got %d, want 2", len(got))
	}
	if err := bs1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	// Reopen — definition loaded, RAM value maps rebuilt from current rels.
	bs2, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer bs2.Close()

	got, err := bs2.RelationshipsByTypeAndProperty(1, "weight", int64(5), QueryOpts{})
	if err != nil {
		t.Fatalf("after reopen: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("after reopen: weight=5 got %d, want 2 (index not rebuilt)", len(got))
	}
	if got, _ := bs2.RelationshipsByTypeAndProperty(1, "weight", int64(9), QueryOpts{}); len(got) != 1 {
		t.Fatalf("after reopen: weight=9 got %d, want 1", len(got))
	}
}

// TestBadgerStoreCreateRelPropertyIndex_ConcurrentWriteVisibility exercises the
// 3-phase creation protocol: a relationship written DURING the (blocked) Phase 2
// backfill must still be captured in the finished index.
func TestBadgerStoreCreateRelPropertyIndex_ConcurrentWriteVisibility(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	putTestNode(t, bs, 1, 1, nil)
	putTestNode(t, bs, 2, 1, nil)

	// Seed enough pre-existing rels that Phase 2 does real work.
	for i := int64(0); i < 50; i++ {
		putRelWithWeight(t, bs, 1000+i, 1, 2, 1, 5)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Concurrent write during index creation — a rel carrying the same value.
		putRelWithWeight(t, bs, 9999, 1, 2, 1, 5)
	}()

	if err := bs.CreateRelPropertyIndex(1, "weight"); err != nil {
		t.Fatalf("CreateRelPropertyIndex: %v", err)
	}
	wg.Wait()

	got, err := bs.RelationshipsByTypeAndProperty(1, "weight", int64(5), QueryOpts{})
	if err != nil {
		t.Fatalf("RelationshipsByTypeAndProperty: %v", err)
	}
	// Every weight=5 rel — the 50 seeded plus the concurrent one — must appear
	// exactly once (no double-count, no omission).
	if len(got) != 51 {
		t.Fatalf("concurrent-write visibility: got %d weight=5 rels, want 51", len(got))
	}
}

// TestBadgerStoreRelPropertyIndex_ConcurrentReadWrite hammers the index with
// concurrent writers, queries, and range scans to shake out data races under
// -race. Correctness is not asserted here (writes race the queries); the point
// is that no access races the RAM index maps.
func TestBadgerStoreRelPropertyIndex_ConcurrentReadWrite(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	putTestNode(t, bs, 1, 1, nil)
	putTestNode(t, bs, 2, 1, nil)
	if err := bs.CreateRelPropertyIndex(1, "weight"); err != nil {
		t.Fatalf("CreateRelPropertyIndex: %v", err)
	}

	var wg sync.WaitGroup
	// Writers.
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(base int64) {
			defer wg.Done()
			for i := int64(0); i < 40; i++ {
				id := 100000 + base*1000 + i
				r := types.NewRelationship(types.RelID(snowflake.ID(id)), 1, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
				_ = r.SetProperty("weight", i%10)
				_ = bs.PutRelationship(r)
			}
		}(int64(w))
	}
	// Readers.
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 40; i++ {
				_, _ = bs.RelationshipsByTypeAndProperty(1, "weight", int64(i%10), QueryOpts{})
				_ = bs.ForEachRelByTypePropertyRange(1, "weight", 2, 6, true, true, QueryOpts{}, func(*types.Relationship) bool { return true })
			}
		}()
	}
	wg.Wait()
}

// TestBadgerStoreRelPropertyIndex_PurgeOnCascade verifies the shared-seam
// brute-force purge removes a rel from the index when it is cascade-deleted with
// its endpoint node (deleteRelByInfo carries no property values).
func TestBadgerStoreRelPropertyIndex_PurgeOnCascade(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	putTestNode(t, bs, 1, 1, nil)
	putTestNode(t, bs, 2, 1, nil)
	putRelWithWeight(t, bs, 10, 1, 2, 1, 5)
	if err := bs.CreateRelPropertyIndex(1, "weight"); err != nil {
		t.Fatalf("CreateRelPropertyIndex: %v", err)
	}

	if err := bs.DeleteNodeCascade(types.NodeID(snowflake.ID(1))); err != nil {
		t.Fatalf("DeleteNodeCascade: %v", err)
	}
	if got, _ := bs.RelationshipsByTypeAndProperty(1, "weight", int64(5), QueryOpts{}); len(got) != 0 {
		t.Fatalf("cascade-deleted rel still in index: %d", len(got))
	}

	// And the RAM index map has no lingering value bucket for the purged ID.
	bs.idxMu.RLock()
	idx := bs.relPropertyIndexes[indexpkg.RelPropertyIndexKey{RelTypeToken: 1, PropertyKey: "weight"}]
	entries := 0
	if idx != nil {
		entries = len(idx.Entries)
	}
	bs.idxMu.RUnlock()
	if entries != 0 {
		t.Fatalf("purge left %d value buckets in the rel property index", entries)
	}
}
