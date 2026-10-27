package badger

import (
	"bytes"
	"fmt"
	"slices"
	"testing"
	"time"

	badgerv4 "github.com/dgraph-io/badger/v4"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// PROPSTR (SCALE, ai-soc's BO run k10270113 on v4.87.2 with
// PropertyIndexOnDisk: 39 of 46 files in 32 min against all 46 in 8 min with
// the RAM index, 91 % in this lookup). A string's on-disk value prefix has no
// terminator, so an equality lookup walked every stored string that extends
// the looked-up one (a folder's paths, a short key's extensions). The walk
// now stays within the stored node IDs' bounds after the value: its visits
// do not grow with the longer strings.
func TestPropStrLookupIgnoresLongerStrings(t *testing.T) {
	const label = uint16(1)
	// visits stores n strings extending "/a" beside "/a" itself and returns
	// the keys one lookup of "/a" visits.
	visits := func(t *testing.T, n int) int {
		bs, err := New(Config{InMemory: true, PropertyIndexOnDisk: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry(),
			FlushInterval: time.Hour, MaxPendingWrites: -1})
		if err != nil {
			t.Fatal(err)
		}
		defer bs.Close()
		if err := bs.CreatePropertyIndex(label, "path"); err != nil {
			t.Fatal(err)
		}
		batch := make([]*types.Node, 0, n+1)
		for i := range n + 1 {
			nd := types.NewNode(types.NodeID(existMapBase+int64(i)), label, nil)
			path := "/a"
			if i > 0 {
				path = fmt.Sprintf("/a/f%d", i)
			}
			_ = nd.SetProperty("path", path)
			batch = append(batch, nd)
		}
		if err := bs.PutNodesBatch(batch); err != nil {
			t.Fatal(err)
		}
		if err := bs.Flush(); err != nil {
			t.Fatal(err)
		}
		// Foreign keys in the node keyspace that are no node row, at both
		// ends: the bounds come from node rows alone.
		if err := bs.DBForTest().Update(func(txn *badgerv4.Txn) error {
			if err := txn.Set([]byte{storepkg.KeyNode}, nil); err != nil {
				return err
			}
			return txn.Set(append([]byte{storepkg.KeyNode}, bytes.Repeat([]byte{0xFF}, storepkg.SizeNodeKey)...), nil)
		}); err != nil {
			t.Fatal(err)
		}
		var seen int
		propertyDiskVisitTestHook = func() { seen++ }
		defer func() { propertyDiskVisitTestHook = nil }()
		if got := nodeIDsVia(t, bs, label, "path", "/a"); !slices.Equal(got, []int64{existMapBase}) {
			t.Fatalf("lookup of /a found %v", got)
		}
		return seen
	}
	small, large := visits(t, 1_000), visits(t, 10_000)
	t.Logf("one lookup visits %d keys beside 1,000 longer strings, %d beside 10,000", small, large)
	if large > 4 {
		t.Fatalf("a lookup visits %d keys beside 10,000 longer strings (%d beside 1,000): it walks them", large, small)
	}
}

// TestPropStrLookupFindsEveryLiveID: the bounded walk still finds the
// value's entries at both ends of the stored node ID range, among longer
// strings, buffered or flushed.
func TestPropStrLookupFindsEveryLiveID(t *testing.T) {
	const label = uint16(1)
	bs, err := New(Config{InMemory: true, PropertyIndexOnDisk: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry(),
		FlushInterval: time.Hour, MaxPendingWrites: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	if err := bs.CreatePropertyIndex(label, "k"); err != nil {
		t.Fatal(err)
	}
	put := func(id int64, v string) {
		t.Helper()
		nd := types.NewNode(types.NodeID(id), label, nil)
		_ = nd.SetProperty("k", v)
		if err := bs.PutNode(nd); err != nil {
			t.Fatal(err)
		}
	}
	const lo, hi = int64(1), int64(1) << 62 // the smallest and a far larger ID
	put(lo, "x")
	put(hi, "x")
	put(500, "xy")
	put(hi-1, "x\x00") // extends "x" by the byte of a small ID's high byte
	check := func(when string) {
		t.Helper()
		if got := nodeIDsVia(t, bs, label, "k", "x"); !slices.Equal(got, []int64{lo, hi}) {
			t.Fatalf("%s: x finds %v, want [%d %d]", when, got, lo, hi)
		}
		if got := nodeIDsVia(t, bs, label, "k", "x\x00"); !slices.Equal(got, []int64{hi - 1}) {
			t.Fatalf("%s: x\\0 finds %v", when, got)
		}
	}
	check("buffered")
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	check("flushed")
}
