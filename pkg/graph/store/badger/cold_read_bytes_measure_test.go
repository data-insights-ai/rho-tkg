//go:build measure

// Measurement level (René 4 Nov, `make measure`): the bytes a read allocates
// are read from the process-wide allocation counter, which other tests,
// badger's own goroutines and host load move, so these bounds run by hand,
// not in a gate (MEMGATE).

package badger

import (
	"runtime"
	"testing"

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
// Mutation: those copies come back at 11.7x.
func TestColdLendNodeBytesBoundedByRowSize(t *testing.T) {
	bs := newTestBadgerStore(t)
	ids := seedColdReadNodes(t, bs, false)
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
	perRow := int(after.TotalAlloc-before.TotalAlloc) / coldReadNodes
	t.Logf("cold LendNode: %d B allocated per row of %d encoded bytes (%.1fx)", perRow, rowBytes, float64(perRow)/float64(rowBytes))
	// -race drops sync.Pool entries on purpose: the bound is for real builds.
	if !raceEnabled && perRow > 7*rowBytes {
		t.Fatalf("cold read allocates %d B per row, want <= 7x the %d encoded bytes", perRow, rowBytes)
	}
}

// TestGetNodeVersionBytesMatchColdRead pins row 121's rewritten-fact shape:
// when an older version wins a pin, hydrating it reads one history row; that
// costs what a cold current-row read costs (one point read, one decode), not a
// second full copy of the freshly decoded row. Mutation: GetNodeVersion's
// DeepCopy of its own decode adds a whole node per call.
func TestGetNodeVersionBytesMatchColdRead(t *testing.T) {
	bs := newTestBadgerStore(t)
	ids := seedColdReadNodes(t, bs, true)
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
		return int(after.TotalAlloc-before.TotalAlloc) / coldReadNodes
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
