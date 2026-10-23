package badger

import (
	"runtime"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// TestColdLendNodeBytesBoundedByRowSize pins row 121 (ai-soc's listing bound,
// cold included): reading one 13-column node row that is not in the entity
// cache allocates at most 7x the row's encoded size (~2.9x is badger's point
// read: the read txn and Get's key and value copies; ~2.5x is the decoded node
// itself). The decode builds the property slice once: no per-property
// validation probe, no canonical re-copy, a pooled wire scratch array.
// Mutation: those copies come back at 11.7x. Serial: it reads the
// process-wide allocation counter.
func TestColdLendNodeBytesBoundedByRowSize(t *testing.T) {
	bs := newTestBadgerStore(t)
	const nodes = 64
	ids := make([]types.NodeID, nodes)
	for i := range ids {
		ps := types.PropertySlice{}
		for c := 0; c < 13; c++ {
			if err := ps.Set("ov_c"+string(rune('a'+c)), "term-value-"+string(rune('a'+c))); err != nil {
				t.Fatal(err)
			}
		}
		n := types.NewNode(types.NodeID(snowflake.ID(1000+i)), 1, nil)
		if err := n.SetProperties(ps); err != nil {
			t.Fatal(err)
		}
		if err := bs.PutNode(n); err != nil {
			t.Fatal(err)
		}
		ids[i] = n.ID()
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	var rowBytes int
	if err := bs.DBForTest().View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(storepkg.NodeKey(ids[0].SnowflakeID()))
		if err != nil {
			return err
		}
		rowBytes = int(item.ValueSize())
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	coldRead := func() {
		for _, id := range ids {
			if _, err := bs.LendNode(id); err != nil {
				t.Fatal(err)
			}
		}
	}
	bs.NodeCacheForTest().ResetForTest()
	coldRead() // warm the badger read path, not the entity cache
	bs.NodeCacheForTest().ResetForTest()
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	coldRead()
	runtime.ReadMemStats(&after)
	perRow := int(after.TotalAlloc-before.TotalAlloc) / nodes
	t.Logf("cold LendNode: %d B allocated per row of %d encoded bytes (%.1fx)", perRow, rowBytes, float64(perRow)/float64(rowBytes))
	// -race drops sync.Pool entries on purpose: the bound is for real builds.
	if !raceEnabled && perRow > 7*rowBytes {
		t.Fatalf("cold read allocates %d B per row, want <= 7x the %d encoded bytes", perRow, rowBytes)
	}
	// The decode must keep every property (an install without the copy must
	// not drop them).
	bs.NodeCacheForTest().ResetForTest()
	got, err := bs.LendNode(ids[0])
	if err != nil {
		t.Fatal(err)
	}
	if got.PropertyCount() != 13 {
		t.Fatalf("cold read returned %d properties, want 13", got.PropertyCount())
	}
	if v, ok := got.GetProperty("ov_cb"); !ok || v != "term-value-b" {
		t.Fatalf("cold read ov_cb = %v, %v; want term-value-b", v, ok)
	}
}

// TestGetNodeVersionBytesMatchColdRead pins row 121's rewritten-fact shape:
// when an older version wins a pin, hydrating it reads one history row; that
// costs what a cold current-row read costs (one point read, one decode), not a
// second full copy of the freshly decoded row. Mutation: GetNodeVersion's
// DeepCopy of its own decode adds a whole node per call. Serial: it reads the
// process-wide allocation counter.
func TestGetNodeVersionBytesMatchColdRead(t *testing.T) {
	bs := newTestBadgerStore(t)
	const nodes = 64
	ids := make([]types.NodeID, nodes)
	for i := range ids {
		ps := types.PropertySlice{}
		for c := 0; c < 13; c++ {
			if err := ps.Set("ov_c"+string(rune('a'+c)), "term-value-"+string(rune('a'+c))); err != nil {
				t.Fatal(err)
			}
		}
		n := types.NewNode(types.NodeID(snowflake.ID(1000+i)), 1, nil)
		if err := n.SetProperties(ps); err != nil {
			t.Fatal(err)
		}
		if err := bs.PutNode(n); err != nil {
			t.Fatal(err)
		}
		if err := bs.PutNodeVersion(n.ID(), 0, n); err != nil {
			t.Fatal(err)
		}
		ids[i] = n.ID()
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	measure := func(read func(types.NodeID)) int {
		for _, id := range ids {
			read(id) // warm the badger read path
		}
		bs.NodeCacheForTest().ResetForTest()
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		for _, id := range ids {
			read(id)
		}
		runtime.ReadMemStats(&after)
		return int(after.TotalAlloc-before.TotalAlloc) / nodes
	}
	cold := measure(func(id types.NodeID) { // measure resets the cache once: each read is cold
		if _, err := bs.LendNode(id); err != nil {
			t.Fatal(err)
		}
	})
	version := measure(func(id types.NodeID) {
		v, err := bs.GetNodeVersion(id, 0)
		if err != nil {
			t.Fatal(err)
		}
		if v.PropertyCount() != 13 {
			t.Fatalf("version has %d properties, want 13", v.PropertyCount())
		}
	})
	t.Logf("GetNodeVersion %d B per read, cold LendNode %d B", version, cold)
	if !raceEnabled && version*10 > cold*13 {
		t.Fatalf("GetNodeVersion allocates %d B per read, want <= 1.3x a cold read (%d B)", version, cold)
	}
}
