package badger

import (
	"fmt"
	"slices"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// PROPBOUNDS (SCALE, ai-soc's BO A/B k10271117 on v4.89.2: the on-disk
// property index is 14 % slower than the RAM one, 59 % of its equality
// lookup in nodeRowIDBounds). A string lookup bounded its walk by the
// smallest and the largest node row, found by a seek from each end of the
// node keyspace on every lookup; badger skips deleted and older versions of
// node rows inside those seeks, so the lookup's cost grew with the rows
// deleted or rewritten at either end.

// TestPropBoundsLookupDoesNotWalkDeletedRows: lookups of absent strings
// beside 20,000 deleted node rows at both ends of the keyspace cost what they
// cost before the deletes.
func TestPropBoundsLookupDoesNotWalkDeletedRows(t *testing.T) {
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
	ids := make([]types.NodeID, 0, n-1)
	for i := range n {
		if i != n/2 { // the one row left sits in the middle
			ids = append(ids, types.NodeID(existMapBase+int64(i)))
		}
	}
	if err := bs.DeleteNodesBatch(ids); err != nil {
		t.Fatal(err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := nodeIDsVia(t, bs, 1, "k", fmt.Sprintf("b%06d", n/2)); len(got) != 1 {
		t.Fatalf("the kept node's value finds %v", got)
	}
	after := lookups()
	t.Logf("%d lookups: %v before the deletes, %v after", probes, before, after)
	if after > 10*before {
		t.Fatalf("%d lookups cost %v after the deletes against %v before: they walk the deleted rows", probes, after, before)
	}
}

// TestPropBoundsAfterReopen: a reopened store's first lookup takes the
// bounds from the stored rows, walks none of 1,000 longer strings sorting
// below them, and a later entry below them widens them.
func TestPropBoundsAfterReopen(t *testing.T) {
	const label = uint16(1)
	dir := t.TempDir()
	reg := registrypkg.NewPropertyKeyRegistry()
	bs, err := New(Config{Dir: dir, PropertyIndexOnDisk: true, PropertyKeyRegistry: reg})
	if err != nil {
		t.Fatal(err)
	}
	if err := bs.CreatePropertyIndex(label, "k"); err != nil {
		t.Fatal(err)
	}
	put := func(bs *Store, id int64, v string) {
		t.Helper()
		nd := types.NewNode(types.NodeID(id), label, nil)
		_ = nd.SetProperty("k", v)
		if err := bs.PutNode(nd); err != nil {
			t.Fatal(err)
		}
	}
	put(bs, existMapBase+1, "/a")
	put(bs, existMapBase+2, "/a")
	for i := range 1_000 { // "/a" then eight bytes below the smallest ID's
		put(bs, existMapBase+10+int64(i), fmt.Sprintf("/a\x00\x00\x00f%d", i))
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	bs, err = New(Config{Dir: dir, PropertyIndexOnDisk: true, PropertyKeyRegistry: reg}) // the owner keeps the registry, as pkg/graph does
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	var seen int
	propertyDiskVisitTestHook = func() { seen++ }
	defer func() { propertyDiskVisitTestHook = nil }()
	if got := nodeIDsVia(t, bs, label, "k", "/a"); !slices.Equal(got, []int64{existMapBase + 1, existMapBase + 2}) {
		t.Fatalf("after reopen /a finds %v", got)
	}
	if seen > 4 {
		t.Fatalf("a lookup after reopen visits %d keys beside 1,000 longer strings: it walks them", seen)
	}
	put(bs, existMapBase, "/a")
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := nodeIDsVia(t, bs, label, "k", "/a"); !slices.Equal(got, []int64{existMapBase, existMapBase + 1, existMapBase + 2}) {
		t.Fatalf("an entry below the bounds: /a finds %v", got)
	}
}
