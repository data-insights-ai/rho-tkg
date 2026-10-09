package index

// Backlog item 10 budget measurement (see bench/index_budget_test.go for the
// graph-level build rate): the exact resident bytes of the two RAM-resident
// structures a badger store (and so each tiered shard) keeps per indexed row,
// isolated from badger's caches. Runs only when RHO_TKG_INDEX_BUDGET_ROWS is
// set, e.g.
//
//	RHO_TKG_INDEX_BUDGET_ROWS=1000000 go test -run TestIndexBudgetStructures -v -count=1 ./pkg/graph/internal/index

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func budgetHeap() uint64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}

func TestIndexBudgetStructures(t *testing.T) {
	v := os.Getenv("RHO_TKG_INDEX_BUDGET_ROWS")
	if v == "" {
		t.Skip("measurement: set RHO_TKG_INDEX_BUDGET_ROWS=<n> to run")
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		t.Fatalf("RHO_TKG_INDEX_BUDGET_ROWS=%q: want a positive integer", v)
	}
	const base = int64(1_790_000_000_000) // ms

	// Rel temporal index: one Extend per relationship (create), as badger's
	// write path and its rebuild do, then one stabbing query so the sorted
	// augmentation (subMax) is materialised as it is after the first read.
	before := budgetHeap()
	ti := NewTemporalIndex()
	for i := range n {
		ti.Extend(snowflake.ID(int64(i)+1), types.Instant(base+int64(i)), 0)
	}
	_ = ti.QueryAt(types.Instant(base + int64(n/2)))
	after := budgetHeap()
	t.Logf("rel temporal index: rows=%d resident %.1f B/row", n, float64(after-before)/float64(n))
	runtime.KeepAlive(ti)
	ti = nil

	// Composite node index over (device, pid, ctime): one distinct tuple per
	// node (ctime distinct), 5000 devices, 65000 pids — ai-soc's shape.
	before = budgetHeap()
	ci := NewCompositePropertyIndex([]string{"device", "pid", "ctime"})
	for i := range n {
		vk := EncodeCompositeKeyTuple([]string{
			types.IndexablePropertyValueKey(fmt.Sprintf("dev-%05d", i%5000)),
			types.IndexablePropertyValueKey(int64(1000 + i%65000)),
			types.IndexablePropertyValueKey(base + int64(i)),
		})
		ci.AddKey(snowflake.ID(int64(i)+1), vk)
	}
	after = budgetHeap()
	t.Logf("composite index (3 keys, distinct tuples): rows=%d resident %.1f B/row", n, float64(after-before)/float64(n))
	runtime.KeepAlive(ci)
}
