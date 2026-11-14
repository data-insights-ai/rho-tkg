//go:build measure

// Measurement level (`make measure`): EMPTYBYID's handle, allocation and time
// cost of the write doors that name a missing ID in a proven-empty shard's window
// (empty_by_id_write_test.go lists them). The allocation counters are process-wide
// (badger's goroutines and host load move them), so the numbers go to
// tasks/evidence/EMPTYBYID/measure.txt, not to a gate; the opens gate in
// TestEmptyByID_RepeatedWriteMissesAndSweepsOpenNothing.

package tiered

import (
	"runtime"
	"testing"
	"time"
)

// TestEmptyByID_MeasureWriteMisses runs rounds of every write miss, each round
// followed by the idle sweep, and reports per round the opens, checkouts and
// writable checkouts of the empty shard, the allocations and the time, and the
// cold handles open when the rounds end.
func TestEmptyByID_MeasureWriteMisses(t *testing.T) {
	f := newEmptyByID(t)
	probes := byIDWriteProbes()
	const rounds = 20
	round := func() {
		for _, p := range probes {
			if err := p.run(f); !p.answers(err) {
				t.Fatalf("%s answered %v, want %v", p.name, err, p.want)
			}
		}
		f.ts.closeIdleShards()
	}
	round() // warm the caches and the first-use paths
	var m0, m1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m0)
	before := f.ts.ShardAccessStats()
	start := time.Now()
	for range rounds {
		round()
	}
	elapsed := time.Since(start)
	runtime.ReadMemStats(&m1)
	after := f.ts.ShardAccessStats()
	n := uint64(rounds)
	t.Logf("per round (%d rounds of %d write misses): empty-shard opens %.2f, cold checkouts %.2f, cold writable checkouts %.2f, "+
		"allocs %d, bytes %d, time %v; cold handles open at the end %+v",
		rounds, len(probes),
		float64(after.ColdOpens-before.ColdOpens)/rounds, float64(after.Cold-before.Cold)/rounds,
		float64(after.ColdWrites-before.ColdWrites)/rounds,
		(m1.Mallocs-m0.Mallocs)/n, (m1.TotalAlloc-m0.TotalAlloc)/n, elapsed/rounds, after.ColdHandles)
}
