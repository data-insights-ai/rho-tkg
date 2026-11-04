//go:build measure

// Measurement level (René 4 Nov, `make measure`): testing.AllocsPerRun and
// runtime.MemStats count every allocation in the process, and this package
// runs parallel tests and badger goroutines, so these bounds run by hand, not
// in a gate (MEMGATE). The rows themselves gate in
// badgerstore_history_decode_test.go.

package badger

import (
	"fmt"
	"math"
	"runtime"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// histDecodeAllocLimits are the per-version ceilings of
// TestHistDecodeAllocsPerVersion (rows of about 400 B, deltaVersionRel /
// deltaVersionNode). Measured 3 Nov (tasks/evidence/RELHIST/p3-red.txt), rel /
// node, N=10 and N=100: before 36.2 / 46.2 and 34.3 / 44.6 allocations, 3,178 /
// 3,618 and 2,990 / 3,460 B; after 30.6 / 39.6 and 28.4 / 37.6, 2,648 / 3,024
// and 2,410 / 2,820 B. The limits sit between: a value copy, a key copy and
// string, a map entry or the row's deep copy per version, back again, fails.
var histDecodeAllocLimits = map[string]struct{ allocs, bytes float64 }{
	"rel":  {allocs: 32, bytes: 2800},
	"node": {allocs: 41, bytes: 3200},
	// The paged doors from version 0 (RelHistoryVersionsFrom /
	// NodeHistoryVersionsFrom), which keep their value copies and lose only the
	// deep copy: before 34.7 / 44.7 and 32.3 / 42.6 allocations, 3,206 / 3,642
	// and 2,964 / 3,434 B; after 31.7 / 40.7 and 29.3 / 38.6, 2,899 / 3,271 and
	// 2,660 / 3,062 B (added after the red commit, measured with the copy back).
	"relpage":  {allocs: 33, bytes: 3050},
	"nodepage": {allocs: 42, bytes: 3400},
}

// TestHistDecodeAllocsPerVersion measures a whole-chain read on badger (rows
// committed, delta encoding on, anchor interval 16) at 10 and 100 versions:
// allocations and bytes per version, rel and node.
func TestHistDecodeAllocsPerVersion(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation counts under -race are not the product's")
	}
	for _, n := range []uint32{10, 100} {
		id := snowflake.ID(43)
		bs := histDecodeStore(t, Config{InMemory: true, HistoryDeltaEncoding: true})
		for v := uint32(0); v < n; v++ {
			if err := bs.PutRelVersion(types.RelID(id), v, deltaVersionRel(id, v)); err != nil {
				t.Fatal(err)
			}
			if err := bs.PutNodeVersion(types.NodeID(id), v, deltaVersionNode(id, v)); err != nil {
				t.Fatal(err)
			}
		}
		if err := bs.Flush(); err != nil {
			t.Fatal(err)
		}
		for _, kind := range []string{"rel", "node", "relpage", "nodepage"} {
			read := func() {
				var err error
				var got int
				switch kind {
				case "rel":
					var rows []*types.Relationship
					rows, err = bs.GetRelHistory(types.RelID(id))
					got = len(rows)
				case "node":
					var rows []*types.Node
					rows, err = bs.GetNodeHistory(types.NodeID(id))
					got = len(rows)
				case "relpage":
					var rows []*types.Relationship
					rows, err = bs.RelHistoryVersionsFrom(types.RelID(id), 0, 0)
					got = len(rows)
				default:
					var rows []*types.Node
					rows, err = bs.NodeHistoryVersionsFrom(types.NodeID(id), 0, 0)
					got = len(rows)
				}
				if err == nil && got != int(n) {
					err = fmt.Errorf("%d rows", got)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			// The counters are process-wide: the least of three rounds.
			const runs = 20
			allocs, perVersion := math.Inf(1), math.Inf(1)
			for round := 0; round < 3; round++ {
				allocs = min(allocs, testing.AllocsPerRun(runs, read)/float64(n))
				var before, after runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&before)
				for i := 0; i < runs; i++ {
					read()
				}
				runtime.ReadMemStats(&after)
				perVersion = min(perVersion, float64(after.TotalAlloc-before.TotalAlloc)/runs/float64(n))
			}
			limit := histDecodeAllocLimits[kind]
			t.Logf("%s N=%d: %.1f allocs/version, %.0f B/version", kind, n, allocs, perVersion)
			if allocs > limit.allocs || perVersion > limit.bytes {
				t.Errorf("%s N=%d: %.1f allocs and %.0f B per version, limit %.0f and %.0f",
					kind, n, allocs, perVersion, limit.allocs, limit.bytes)
			}
		}
	}
}
