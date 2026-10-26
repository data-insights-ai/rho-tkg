package badger

import (
	"os"
	"os/exec"
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

// existMapChildEnv marks the child process TestExistMapResidentBytesPerNode
// measures in.
const existMapChildEnv = "RHO_EXISTMAP_CHILD"

// TestExistMapResidentBytesPerNode pins EXISTMAP's budget (SCALE, lesson 89):
// an open shard may keep at most 12 B of live heap per stored node (v4.83.0:
// ~83 B live, ~118 B of in-use spans, mostly the nodeIDs and nodeRevs maps,
// so a 4-billion-fact day-shard needed ~250 GB). Measured as live bytes after
// a GC (HeapAlloc; HeapInuse adds span slack) and as the slope between two
// store sizes after a clean reopen, so the fixed costs cancel. It measures in
// a fresh run of the test binary: in the package's own process the heap that
// earlier tests' stores still hold moves by megabytes between the two reads
// (20 B per node read there, 6.5-8.4 B alone). The race detector's shadow
// allocations get their own bound, still far below the per-entity maps.
func TestExistMapResidentBytesPerNode(t *testing.T) {
	if os.Getenv(existMapChildEnv) == "" {
		if testing.Short() && !raceEnabled {
			t.Skip("builds and reopens two disk stores of 50 k and 250 k nodes")
		}
		cmd := exec.Command(os.Args[0], "-test.run=^TestExistMapResidentBytesPerNode$", "-test.count=1", "-test.v") // #nosec G204 -- the test binary itself
		cmd.Env = append(os.Environ(), existMapChildEnv+"=1")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("measuring child: %v\n%s", err, out)
		}
		t.Logf("%s", out)
		return
	}
	const small, large = 50_000, 250_000
	hs := existMapHeapAfterReopen(t, small)
	hl := existMapHeapAfterReopen(t, large)
	perNode := (float64(hl) - float64(hs)) / (large - small)
	t.Logf("reopened store: %d B at %d nodes, %d B at %d nodes: %.1f B per node", hs, small, hl, large, perNode)
	budget := 12.0
	if raceEnabled {
		budget = 30
	}
	if perNode > budget {
		t.Fatalf("an open store keeps %.1f B of heap per node, budget %.0f B", perNode, budget)
	}
}
