package tiered

import (
	"testing"

	badgerpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// resetLendFillCaches flushes the store and empties the node and rel caches
// of every open shard, so the next lend reads each row from badger.
func resetLendFillCaches(t *testing.T, f *lendBatchFixture) {
	t.Helper()
	if err := f.ts.Flush(); err != nil {
		t.Fatal(err)
	}
	stores := []*BadgerStore{f.ts.refShard, f.ts.refArchive.Load()}
	f.ts.mu.RLock()
	for _, es := range f.ts.eventShards {
		stores = append(stores, es.store)
	}
	f.ts.mu.RUnlock()
	for _, s := range stores {
		if s != nil {
			s.NodeCacheForTest().ResetForTest()
			s.RelCacheForTest().ResetForTest()
		}
	}
}

// TestTieredLendSortedFillsTheCache (LENDCOST round 2): tiered's batch lend
// caches what it reads, as its per-ID path did up to v4.102.0 (badger
// LendNode: a hit promoted, a miss filled), so a second lend of the same IDs
// decodes no row; the rows equal the first lend's. After a write and a flush
// the lend returns the new version, then serves it from the cache. Not
// parallel: the decode counter is process-wide.
func TestTieredLendSortedFillsTheCache(t *testing.T) {
	f := newLendBatchFixture(t, false)
	resetLendFillCaches(t, f)
	nids := f.nodeIDs(f.liveN)
	rids := f.relIDs(f.liveR)
	lendNodes := func() []*types.Node {
		out := make([]*types.Node, len(nids))
		if err := f.ts.LendNodesSorted(nids, out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	lendRels := func() []*types.Relationship {
		out := make([]*types.Relationship, len(rids))
		if err := f.ts.LendRelationshipsSorted(rids, out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	var first, second []*types.Node
	if n := badgerpkg.CountRowDecodesForTest(func() { first = lendNodes() }); n < int64(len(nids)) {
		t.Fatalf("first node lend decoded %d rows, want at least %d (caches emptied)", n, len(nids))
	}
	if n := badgerpkg.CountRowDecodesForTest(func() { second = lendNodes() }); n != 0 {
		t.Fatalf("second node lend of the same IDs decoded %d rows, want 0 (cache hits)", n)
	}
	for i := range nids {
		if nodeRowKey(first[i]) != nodeRowKey(second[i]) || second[i] == nil || !second[i].IsFrozen() {
			t.Fatalf("node %s: first %s, second %s", f.liveN[i], nodeRowKey(first[i]), nodeRowKey(second[i]))
		}
	}
	var rfirst, rsecond []*types.Relationship
	if n := badgerpkg.CountRowDecodesForTest(func() { rfirst = lendRels() }); n < int64(len(rids)) {
		t.Fatalf("first rel lend decoded %d rows, want at least %d (caches emptied)", n, len(rids))
	}
	if n := badgerpkg.CountRowDecodesForTest(func() { rsecond = lendRels() }); n != 0 {
		t.Fatalf("second rel lend of the same IDs decoded %d rows, want 0 (cache hits)", n)
	}
	for i := range rids {
		if relRowKey(rfirst[i]) != relRowKey(rsecond[i]) || rsecond[i] == nil {
			t.Fatalf("rel %s: first %s, second %s", f.liveR[i], relRowKey(rfirst[i]), relRowKey(rsecond[i]))
		}
	}

	// Two-phase: a replaced and flushed row is lent in its new version.
	hotA := f.nodes["hotA"]
	n := types.NewNode(hotA, mintSignalTok, nil)
	if err := n.SetProperty("v", "hotA-2"); err != nil {
		t.Fatal(err)
	}
	if err := f.ts.ReplaceNode(n); err != nil {
		t.Fatal(err)
	}
	if err := f.ts.Flush(); err != nil {
		t.Fatal(err)
	}
	third := lendNodes()
	for i, name := range f.liveN {
		want := name
		if name == "hotA" {
			want = "hotA-2"
		}
		if v, _ := third[i].GetProperty("v"); v != want {
			t.Fatalf("node %s after the replace: v %v, want %s", name, v, want)
		}
	}
}
