package badger

import (
	"fmt"
	"slices"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// PROPSEEK (SCALE, ai-soc's BO run k10270452-p4b on v4.89.0 with
// PropertyIndexOnDisk: stalled at 37 of 55 files, 91 % of the CPU in the
// on-disk equality lookup, 99.9 % of that inside badger's Iterator.Seek). A
// badger iterator without a prefix skips deleted entries and older versions
// inside Seek until it holds two live keys, wherever they are; rho's bound
// on the walk applies only to the keys Seek returns. A lookup of a value that
// sorts before the deleted entries of a rewritten key range therefore walked
// all of them, on every get-or-create.

// propSeekStore holds n nodes of label 1 whose indexed "k" is b<i>.
func propSeekStore(t *testing.T, n int) *Store {
	t.Helper()
	bs, err := New(Config{InMemory: true, PropertyIndexOnDisk: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry(),
		FlushInterval: time.Hour, MaxPendingWrites: -1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	if err := bs.CreatePropertyIndex(1, "k"); err != nil {
		t.Fatal(err)
	}
	batch := make([]*types.Node, 0, n)
	for i := range n {
		nd := types.NewNode(types.NodeID(existMapBase+int64(i)), 1, nil)
		_ = nd.SetProperty("k", fmt.Sprintf("b%06d", i))
		batch = append(batch, nd)
	}
	if err := bs.PutNodesBatch(batch); err != nil {
		t.Fatal(err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	return bs
}

// TestPropSeekLookupDoesNotWalkDeletedEntries: lookups of absent values that
// sort before 20,000 rewritten keys cost what they cost before the rewrite.
func TestPropSeekLookupDoesNotWalkDeletedEntries(t *testing.T) {
	const n, probes = 20_000, 200
	bs := propSeekStore(t, n)
	lookups := func() time.Duration {
		best := time.Duration(1<<63 - 1)
		for range 3 {
			start := time.Now()
			for i := range probes {
				if got := nodeIDsVia(t, bs, 1, "k", fmt.Sprintf("a%06d", i)); len(got) != 0 {
					t.Fatalf("absent value found %v", got)
				}
			}
			best = min(best, time.Since(start))
		}
		return best
	}
	before := lookups()
	for i := range n { // every b<i> entry is deleted, c<i> written
		nd := types.NewNode(types.NodeID(existMapBase+int64(i)), 1, nil)
		_ = nd.SetProperty("k", fmt.Sprintf("c%06d", i))
		if err := bs.ReplaceNode(nd); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := nodeIDsVia(t, bs, 1, "k", "b000007"); len(got) != 0 {
		t.Fatalf("a rewritten value still found: %v", got)
	}
	after := lookups()
	t.Logf("%d lookups: %v before the rewrite, %v after (%.1fx)", probes, before, after, float64(after)/float64(before))
	if after > 10*before {
		t.Fatalf("%d lookups take %v after 20,000 entries were deleted, %v before: they walk the deleted entries", probes, after, before)
	}
}

// TestPropSeekNewestVersionDecides: the lookup reads every version of an
// entry; only the newest counts. Each step is flushed on its own, so badger
// holds the versions apart.
func TestPropSeekNewestVersionDecides(t *testing.T) {
	bs := propSeekStore(t, 0)
	put := func(i int64, v string, replace bool) {
		t.Helper()
		nd := types.NewNode(types.NodeID(existMapBase+i), 1, nil)
		_ = nd.SetProperty("k", v)
		var err error
		if replace {
			err = bs.ReplaceNode(nd)
		} else {
			err = bs.PutNode(nd)
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := bs.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	for i := range int64(5) {
		put(i, "v", false)
	}
	put(1, "w", true) // v|1: set, deleted
	put(1, "v", true) // ... and set again
	put(2, "w", true) // v|2: set, deleted
	if err := bs.DeleteNode(types.NodeID(existMapBase + 3)); err != nil {
		t.Fatal(err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	// The door rechecks each node's value, so the index's candidates are
	// checked as well: a stale one would cost a node read per lookup.
	candidates := func(v string) []int64 {
		bs.idxMu.RLock()
		defer bs.idxMu.RUnlock()
		nids, err := bs.propertyIndexDiskKeysCandidatesLocked("k", []string{types.IndexablePropertyValueKey(v)})
		if err != nil {
			t.Fatal(err)
		}
		ids := make([]int64, 0, len(nids))
		for _, id := range nids {
			ids = append(ids, int64(id))
		}
		slices.Sort(ids)
		return ids
	}
	for v, want := range map[string][]int64{"v": {existMapBase, existMapBase + 1, existMapBase + 4}, "w": {existMapBase + 2}} {
		if got := nodeIDsVia(t, bs, 1, "k", v); !slices.Equal(got, want) {
			t.Fatalf("%s: got %v, want %v", v, got, want)
		}
		if got := candidates(v); !slices.Equal(got, want) {
			t.Fatalf("%s: candidates %v, want %v", v, got, want)
		}
	}
}
