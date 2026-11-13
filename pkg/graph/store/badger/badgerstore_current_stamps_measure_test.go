//go:build measure

// Measurement level (René 4 Nov, `scripts/measure.sh`): testing.AllocsPerRun counts
// every allocation in the process, and this package runs parallel tests and
// badger goroutines, so these bounds run by hand, not in a gate (MEMGATE). The
// decode and cache counts gate in TestCurrentStamps_MissReadsTheTemporalBlockOnly.

package badger

import (
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// TestCurrentStamps_MissAllocs: a cache miss reads one key and decodes the
// temporal block only: a handful of allocations (the badger read
// transaction), not the full row decode's 20+ (properties, labels, hashes).
// The bound is the backlog-42 target for the whole LatestStamps call; a hit
// allocates nothing.
func TestCurrentStamps_MissAllocs(t *testing.T) {
	for _, k := range curKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := curStampsMissStore(t, k)
			const a = curStampsMissID
			var got storecontract.RowStamps
			var err error
			allocs := testing.AllocsPerRun(200, func() {
				k.evict(bs, a)
				got, err = k.stamps(bs, a)
			})
			if err != nil || got != (storecontract.RowStamps{TxFrom: 100, Version: 3}) {
				t.Fatalf("miss = (%+v, %v)", got, err)
			}
			if allocs > 8 {
				t.Fatalf("a cache miss allocates %.0f per call, want <= 8 (temporal block only)", allocs)
			}
			if _, err := k.get(bs, a); err != nil { // GetNode fills the cache; the miss path does not
				t.Fatal(err)
			}
			if hit := testing.AllocsPerRun(200, func() { got, err = k.stamps(bs, a) }); hit != 0 {
				t.Fatalf("a cache hit allocates %.0f per call, want 0", hit)
			}
		})
	}
}
