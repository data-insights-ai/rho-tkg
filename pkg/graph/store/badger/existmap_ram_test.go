package badger

import (
	"runtime"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// existMapBase is a snowflake-shaped ID base: the IDs ascend as minted ones do.
const existMapBase = int64(1) << 42

// existMapHeapAfterReopen builds a disk store of n labelled nodes, closes it
// cleanly (open image), and returns the live heap the reopened store holds. The
// store runs as a tiered shard of ai-soc's (label and adjacency indexes on
// disk, no ordinals); the block and index caches are pinned at 1 MB and the
// memtable at its minimum, so the fixed costs do not depend on n.
func existMapHeapAfterReopen(t *testing.T, n int) uint64 {
	t.Helper()
	cfg := Config{Dir: t.TempDir(), BlockCacheSize: 1 << 20, IndexCacheSize: 1 << 20, MemTableSize: 8 << 20,
		LabelIndexOnDisk: true, AdjacencyIndexOnDisk: true, DisableOrdinals: true}
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	batch := make([]*types.Node, 0, 1000)
	for i := range n {
		batch = append(batch, types.NewNode(types.NodeID(existMapBase+int64(i)), 1, nil))
		if len(batch) == cap(batch) || i == n-1 {
			if err := bs.PutNodesBatch(batch); err != nil {
				t.Fatal(err)
			}
			batch = batch[:0]
		}
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	var m runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m)
	before := m.HeapAlloc
	bs, err = New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	runtime.GC()
	runtime.ReadMemStats(&m)
	if m.HeapAlloc < before {
		return 0
	}
	return m.HeapAlloc - before
}

// TestExistMapResidentBytesPerNode pins EXISTMAP's budget (SCALE, lesson 89):
// an open shard may keep at most 12 B of live heap per stored node (v4.83.0:
// ~83 B live, ~118 B of in-use spans, mostly the nodeIDs and nodeRevs maps,
// so a 4-billion-fact day-shard needed ~250 GB). Measured as live bytes after
// a GC (HeapAlloc; HeapInuse adds span slack) and as the slope between two
// store sizes after a clean reopen, so the fixed costs cancel. Serial: it
// reads runtime.MemStats.
func TestExistMapResidentBytesPerNode(t *testing.T) {
	if testing.Short() && !raceEnabled {
		t.Skip("builds and reopens two disk stores of 50 k and 250 k nodes")
	}
	const small, large = 50_000, 250_000
	hs := existMapHeapAfterReopen(t, small)
	hl := existMapHeapAfterReopen(t, large)
	perNode := (float64(hl) - float64(hs)) / (large - small)
	t.Logf("reopened store: %d B at %d nodes, %d B at %d nodes: %.1f B per node", hs, small, hl, large, perNode)
	if perNode > 12 {
		t.Fatalf("an open store keeps %.1f B of heap per node, budget 12 B", perNode)
	}
}
