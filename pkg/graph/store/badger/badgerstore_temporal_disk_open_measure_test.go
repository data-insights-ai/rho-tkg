//go:build measure

// Measurement level: wall-clock open times move with host load, so this runs
// by hand, not in a gate:
// go test -tags measure -run TestTemporalIndexOnDisk_OpenTimeMeasurement_100k ./pkg/graph/store/badger/

package badger

import (
	"fmt"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestTemporalIndexOnDisk_OpenTimeMeasurement_100k prepares an on-disk store
// with 100,000 nodes under one temporal-indexed label, clones the directory,
// and times a reopen with TemporalIndexOnDisk off (the full node fetch and
// decode rebuild) against one with it on (the compact 0x0B prefix stream). It
// asserts only that both reopens answer a ValidAt probe alike and logs the two
// durations; the numbers are the point.
//
// Measured once (Apple Silicon): about 1.8x (795 ms -> 452 ms, about six
// properties per node). The first open pass decodes every node row whatever
// the flag, so the fast path removes only the second, temporal-index decode
// pass; each further temporal-indexed label would add one more such pass to
// the slow path.
func TestTemporalIndexOnDisk_OpenTimeMeasurement_100k(t *testing.T) {
	t.Parallel()
	const label = uint16(1)
	const n = 100_000

	dir := t.TempDir()
	bs, err := New(Config{Dir: dir, TemporalIndexOnDisk: true, FlushInterval: 1<<63 - 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := bs.CreateTemporalIndex(label); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}

	// A typical payload: the slow rebuild decodes the full row, so bare nodes
	// would understate its cost.
	nodes := make([]*types.Node, n)
	for i := 0; i < n; i++ {
		nd := types.NewNode(types.NodeID(snowflake.ID(i+1)), label, nil)
		nd.SetTemporal(&types.TemporalMetadata{ValidFrom: types.Instant(1 + i), ValidTo: 0})
		if err := nd.SetProperty("name", fmt.Sprintf("entity-%d", i)); err != nil {
			t.Fatalf("SetProperty(name): %v", err)
		}
		if err := nd.SetProperty("description", "a moderately sized description field simulating a realistic entity payload for the rebuild-at-open cost measurement"); err != nil {
			t.Fatalf("SetProperty(description): %v", err)
		}
		if err := nd.SetProperty("score", float64(i%1000)/10.0); err != nil {
			t.Fatalf("SetProperty(score): %v", err)
		}
		if err := nd.SetProperty("count", int64(i)); err != nil {
			t.Fatalf("SetProperty(count): %v", err)
		}
		if err := nd.SetProperty("active", i%2 == 0); err != nil {
			t.Fatalf("SetProperty(active): %v", err)
		}
		if err := nd.SetProperty("category", fmt.Sprintf("cat-%d", i%20)); err != nil {
			t.Fatalf("SetProperty(category): %v", err)
		}
		nodes[i] = nd
	}
	if err := bs.PutNodesBatch(nodes); err != nil {
		t.Fatalf("PutNodesBatch: %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	dirFast := t.TempDir()
	copyDirFiles(t, dir, dirFast)

	t0 := time.Now()
	slow, err := New(Config{Dir: dir, TemporalIndexOnDisk: false})
	slowElapsed := time.Since(t0)
	if err != nil {
		t.Fatalf("reopen (flag off, slow rebuild): %v", err)
	}
	defer slow.Close()

	t1 := time.Now()
	fast, err := New(Config{Dir: dirFast, TemporalIndexOnDisk: true})
	fastElapsed := time.Since(t1)
	if err != nil {
		t.Fatalf("reopen (flag on, fast rebuild): %v", err)
	}
	defer fast.Close()

	// The timing means something only if both reopens rebuilt the same index.
	probe := types.Instant(n / 2)
	gotSlow := nodeIDsValidAt(t, slow, label, probe)
	gotFast := nodeIDsValidAt(t, fast, label, probe)
	if len(gotSlow) != len(gotFast) {
		t.Fatalf("open-time measurement: result size mismatch slow=%d fast=%d entries at ValidAt=%d",
			len(gotSlow), len(gotFast), probe)
	}

	t.Logf("open-time measurement (%d entities, 1 temporal-indexed label):", n)
	t.Logf("  TemporalIndexOnDisk=false (full node fetch+decode per entity): %v", slowElapsed)
	t.Logf("  TemporalIndexOnDisk=true  (compact 0x0B prefix stream):        %v", fastElapsed)
	if fastElapsed > 0 {
		t.Logf("  speedup: %.1fx", float64(slowElapsed)/float64(fastElapsed))
	}
}
