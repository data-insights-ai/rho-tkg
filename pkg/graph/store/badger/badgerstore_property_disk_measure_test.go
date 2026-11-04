//go:build measure

// Measurement level (René 4 Nov, `make measure`): testing.AllocsPerRun counts
// every allocation in the process, and this package runs parallel tests and
// badger goroutines, so these bounds run by hand, not in a gate (MEMGATE).

package badger

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestPropertyIndexOnDisk_BuildsIndexedKeysOnlyAllocs (IDXKEYLAZY): the
// on-disk index's write path and key-size check, with a label listed or every
// label on disk, build the value key of an indexed property only; before,
// every property of the row got one per label, indexed or not. DISKDEFS: a
// key within the limit is measured, not built.
func TestPropertyIndexOnDisk_BuildsIndexedKeysOnlyAllocs(t *testing.T) {
	forEachIndexedKeysOnlyCase(t, func(cfg Config, bs *Store, n *types.Node) {
		for name, run := range map[string]func(){
			"write path": func() { _ = bs.propertyIndexDiskMaintain(n, 1, writeOpSet) },
			"key size":   func() { _ = bs.checkNodeIndexKeysLocked(n, 1) },
		} {
			if allocs := testing.AllocsPerRun(50, run); allocs >= indexedKeysOnlyUnindexed/2 {
				t.Errorf("%v %s: %.0f allocations with %d unindexed properties", cfg.PropertyIndexOnDiskLabels, name, allocs, indexedKeysOnlyUnindexed)
			}
		}
		valueKey := testing.AllocsPerRun(50, func() { _, _ = n.IndexablePropertyValueKey("k") })
		if allocs := testing.AllocsPerRun(50, func() { _ = bs.checkNodeIndexKeysLocked(n, 1) }); allocs > valueKey {
			t.Errorf("%v key size: %.0f allocations, the value key alone %.0f", cfg.PropertyIndexOnDiskLabels, allocs, valueKey)
		}
	})
}
