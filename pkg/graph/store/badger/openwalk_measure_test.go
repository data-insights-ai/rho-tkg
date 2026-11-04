//go:build measure

// Measurement level (René 4 Nov, `make measure`): live heap after a GC moves
// with load and other goroutines, so this bound runs by hand, not in a gate
// (MEMGATE). The keyspace-walk counts of OPENWALK gate in openwalk_test.go.

package badger

import (
	"os"
	"os/exec"
	"runtime"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// openWalkHeapAfterReopen builds a disk store of 1,000 nodes and n rels of two
// types between them, closes it cleanly and returns the live heap the
// reopened store holds (as existMapHeapAfterReopen, ai-soc's shard config).
func openWalkHeapAfterReopen(t *testing.T, n int) uint64 {
	t.Helper()
	cfg := Config{Dir: t.TempDir(), BlockCacheSize: 1 << 20, IndexCacheSize: 1 << 20, MemTableSize: 8 << 20,
		LabelIndexOnDisk: true, AdjacencyIndexOnDisk: true, DisableOrdinals: true}
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	const nodes = 1_000
	batch := make([]*types.Node, 0, nodes)
	for i := range nodes {
		batch = append(batch, types.NewNode(types.NodeID(existMapBase+int64(i)), 1, nil))
	}
	if err := bs.PutNodesBatch(batch); err != nil {
		t.Fatal(err)
	}
	rels := make([]*types.Relationship, 0, 1000)
	for i := range n {
		start, end := existMapBase+int64(i%nodes), existMapBase+int64((i+1)%nodes)
		rels = append(rels, types.NewRelationship(types.RelID(existMapBase+nodes+int64(i)), uint16(7+i%2), types.NodeID(start), types.NodeID(end)))
		if len(rels) == cap(rels) || i == n-1 {
			if err := bs.PutRelationshipsBatch(rels); err != nil {
				t.Fatal(err)
			}
			rels = rels[:0]
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

const openWalkChildEnv = "RHO_OPENWALK_CHILD"

// TestOpenWalkResidentBytesPerRel pins OPENWALK's RAM budget (SCALE, lesson
// 89): an open shard keeps at most 24 B of live heap per stored rel (the rel
// ID set and the rel-type index as ID sets; v4.86.1: ~45 B, the type index a
// map[RelID]struct{} per type). Measured as TestExistMapResidentBytesPerNode:
// the slope between two store sizes after a clean reopen, in a fresh process.
func TestOpenWalkResidentBytesPerRel(t *testing.T) {
	if os.Getenv(openWalkChildEnv) == "" {
		if testing.Short() && !raceEnabled {
			t.Skip("builds and reopens two disk stores of 50 k and 250 k rels")
		}
		cmd := exec.Command(os.Args[0], "-test.run=^TestOpenWalkResidentBytesPerRel$", "-test.count=1", "-test.v") // #nosec G204 -- the test binary itself
		cmd.Env = append(os.Environ(), openWalkChildEnv+"=1")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("measuring child: %v\n%s", err, out)
		}
		t.Logf("%s", out)
		return
	}
	const small, large = 50_000, 250_000
	hs := openWalkHeapAfterReopen(t, small)
	hl := openWalkHeapAfterReopen(t, large)
	perRel := (float64(hl) - float64(hs)) / (large - small)
	t.Logf("reopened store: %d B at %d rels, %d B at %d rels: %.1f B per rel", hs, small, hl, large, perRel)
	budget := 24.0
	if raceEnabled {
		budget = 60
	}
	if perRel > budget {
		t.Fatalf("an open store keeps %.1f B of heap per rel, budget %.0f B", perRel, budget)
	}
}
