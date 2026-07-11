package memory

import (
	"errors"
	"sync"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestMemoryStoreNodePropertyStats(t *testing.T) {
	t.Parallel()
	ms := New()
	t.Cleanup(func() { _ = ms.Close() })

	n1 := types.NewNode(types.NodeID(snowflake.ID(101)), 1, nil)
	if err := n1.SetProperty("score", int64(10)); err != nil {
		t.Fatalf("SetProperty n1: %v", err)
	}
	if err := ms.PutNode(n1); err != nil {
		t.Fatalf("PutNode n1: %v", err)
	}
	n2 := types.NewNode(types.NodeID(snowflake.ID(102)), 1, nil)
	if err := n2.SetProperty("score", int64(30)); err != nil {
		t.Fatalf("SetProperty n2: %v", err)
	}
	if err := ms.PutNode(n2); err != nil {
		t.Fatalf("PutNode n2: %v", err)
	}
	n3 := types.NewNode(types.NodeID(snowflake.ID(103)), 1, nil)
	if err := n3.SetProperty("score", int64(20)); err != nil {
		t.Fatalf("SetProperty n3: %v", err)
	}
	if err := ms.PutNode(n3); err != nil {
		t.Fatalf("PutNode n3: %v", err)
	}

	stats, err := ms.NodePropertyStats(1, "score")
	if err != nil {
		t.Fatalf("NodePropertyStats: %v", err)
	}
	if stats.Count != 3 {
		t.Fatalf("Count = %d, want 3", stats.Count)
	}
	if stats.Min != int64(10) {
		t.Fatalf("Min = %v, want int64(10)", stats.Min)
	}
	if stats.Max != int64(30) {
		t.Fatalf("Max = %v, want int64(30)", stats.Max)
	}
	if stats.NDV < 1 || stats.NDV > 10 {
		t.Fatalf("NDV = %d, want a small positive estimate (3 distinct values)", stats.NDV)
	}
}

// TestMemoryStoreNodePropertyStatsMissingPairReturnsZeroValue mirrors
// NodeCountByLabelAndPropertyKey's "unregistered → 0, not an error"
// convention for the richer PropertyStats sibling.
func TestMemoryStoreNodePropertyStatsMissingPairReturnsZeroValue(t *testing.T) {
	t.Parallel()
	ms := New()
	t.Cleanup(func() { _ = ms.Close() })

	stats, err := ms.NodePropertyStats(99, "nope")
	if err != nil {
		t.Fatalf("NodePropertyStats missing pair: %v", err)
	}
	if stats != (storecontract.PropertyStats{}) {
		t.Fatalf("stats = %+v, want zero value", stats)
	}

	// A label that HAS other property-key stats, but not this key.
	n := types.NewNode(types.NodeID(snowflake.ID(201)), 5, nil)
	if err := n.SetProperty("known", int64(1)); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	stats, err = ms.NodePropertyStats(5, "unknown-key")
	if err != nil {
		t.Fatalf("NodePropertyStats unknown key on populated label: %v", err)
	}
	if stats != (storecontract.PropertyStats{}) {
		t.Fatalf("stats = %+v, want zero value", stats)
	}
}

// TestMemoryStoreNodePropertyStatsNonIndexableValueExcluded asserts a
// non-scalar property value (e.g. a []string) contributes to neither Count
// nor NDV nor Min/Max — matching the presence counter's own "indexable
// scalar only" contract (rule 17: two doors, same shape audited here for a
// THIRD door on the same underlying predicate).
func TestMemoryStoreNodePropertyStatsNonIndexableValueExcluded(t *testing.T) {
	t.Parallel()
	ms := New()
	t.Cleanup(func() { _ = ms.Close() })

	n := types.NewNode(types.NodeID(snowflake.ID(301)), 3, nil)
	if err := n.SetProperty("tags", []string{"a", "b"}); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	stats, err := ms.NodePropertyStats(3, "tags")
	if err != nil {
		t.Fatalf("NodePropertyStats: %v", err)
	}
	if stats != (storecontract.PropertyStats{}) {
		t.Fatalf("stats = %+v, want zero value for a non-indexable property", stats)
	}
}

// TestMemoryStoreNodePropertyStatsDeleteExtremumTriggersRescan is the
// delete-the-extremum two-phase test: node holding the current MAX is
// deleted; NodePropertyStats must report the new exact max over the
// SURVIVING nodes, not the stale deleted value.
func TestMemoryStoreNodePropertyStatsDeleteExtremumTriggersRescan(t *testing.T) {
	t.Parallel()
	ms := New()
	t.Cleanup(func() { _ = ms.Close() })

	low := types.NewNode(types.NodeID(snowflake.ID(401)), 1, nil)
	if err := low.SetProperty("v", int64(1)); err != nil {
		t.Fatalf("SetProperty low: %v", err)
	}
	if err := ms.PutNode(low); err != nil {
		t.Fatalf("PutNode low: %v", err)
	}
	mid := types.NewNode(types.NodeID(snowflake.ID(402)), 1, nil)
	if err := mid.SetProperty("v", int64(5)); err != nil {
		t.Fatalf("SetProperty mid: %v", err)
	}
	if err := ms.PutNode(mid); err != nil {
		t.Fatalf("PutNode mid: %v", err)
	}
	extremum := types.NewNode(types.NodeID(snowflake.ID(403)), 1, nil)
	if err := extremum.SetProperty("v", int64(100)); err != nil {
		t.Fatalf("SetProperty extremum: %v", err)
	}
	if err := ms.PutNode(extremum); err != nil {
		t.Fatalf("PutNode extremum: %v", err)
	}

	// Phase 1: confirm max=100 before the delete.
	before, err := ms.NodePropertyStats(1, "v")
	if err != nil {
		t.Fatalf("NodePropertyStats (before): %v", err)
	}
	if before.Max != int64(100) {
		t.Fatalf("Max (before) = %v, want int64(100)", before.Max)
	}
	if before.Min != int64(1) {
		t.Fatalf("Min (before) = %v, want int64(1)", before.Min)
	}

	// Phase 2: delete the max holder.
	if err := ms.DeleteNode(extremum.ID()); err != nil {
		t.Fatalf("DeleteNode extremum: %v", err)
	}

	after, err := ms.NodePropertyStats(1, "v")
	if err != nil {
		t.Fatalf("NodePropertyStats (after): %v", err)
	}
	if after.Max != int64(5) {
		t.Fatalf("Max (after delete-the-extremum) = %v, want int64(5) — must NOT still report the deleted 100", after.Max)
	}
	if after.Min != int64(1) {
		t.Fatalf("Min (after) = %v, want int64(1) (unaffected)", after.Min)
	}
	if after.Count != 2 {
		t.Fatalf("Count (after) = %d, want 2", after.Count)
	}

	// Delete the (now-)min holder too — rescan over the single survivor.
	if err := ms.DeleteNode(low.ID()); err != nil {
		t.Fatalf("DeleteNode low: %v", err)
	}
	final, err := ms.NodePropertyStats(1, "v")
	if err != nil {
		t.Fatalf("NodePropertyStats (final): %v", err)
	}
	if final.Min != int64(5) || final.Max != int64(5) {
		t.Fatalf("Min/Max (final) = %v/%v, want both int64(5)", final.Min, final.Max)
	}
	if final.Count != 1 {
		t.Fatalf("Count (final) = %d, want 1", final.Count)
	}

	// Delete the last node — the pair goes back to empty.
	if err := ms.DeleteNode(mid.ID()); err != nil {
		t.Fatalf("DeleteNode mid: %v", err)
	}
	empty, err := ms.NodePropertyStats(1, "v")
	if err != nil {
		t.Fatalf("NodePropertyStats (empty): %v", err)
	}
	if empty.Min != nil || empty.Max != nil {
		t.Fatalf("Min/Max (empty) = %v/%v, want both nil", empty.Min, empty.Max)
	}
	if empty.Count != 0 {
		t.Fatalf("Count (empty) = %d, want 0", empty.Count)
	}
}

// TestMemoryStoreNodePropertyStatsReplaceNodeUpdatesExtremum exercises the
// ReplaceNode path (as opposed to DeleteNode) invalidating the extremum.
func TestMemoryStoreNodePropertyStatsReplaceNodeUpdatesExtremum(t *testing.T) {
	t.Parallel()
	ms := New()
	t.Cleanup(func() { _ = ms.Close() })

	n1 := types.NewNode(types.NodeID(snowflake.ID(501)), 2, nil)
	if err := n1.SetProperty("v", int64(1)); err != nil {
		t.Fatalf("SetProperty n1: %v", err)
	}
	if err := ms.PutNode(n1); err != nil {
		t.Fatalf("PutNode n1: %v", err)
	}
	n2 := types.NewNode(types.NodeID(snowflake.ID(502)), 2, nil)
	if err := n2.SetProperty("v", int64(50)); err != nil {
		t.Fatalf("SetProperty n2: %v", err)
	}
	if err := ms.PutNode(n2); err != nil {
		t.Fatalf("PutNode n2: %v", err)
	}

	// Replace n2 (the max holder) with a lower value.
	updated := n2.DeepCopy()
	if err := updated.SetProperty("v", int64(2)); err != nil {
		t.Fatalf("SetProperty updated: %v", err)
	}
	if err := ms.ReplaceNode(updated); err != nil {
		t.Fatalf("ReplaceNode: %v", err)
	}

	stats, err := ms.NodePropertyStats(2, "v")
	if err != nil {
		t.Fatalf("NodePropertyStats: %v", err)
	}
	if stats.Max != int64(2) {
		t.Fatalf("Max = %v, want int64(2) (the old max value 50 must not survive the replace)", stats.Max)
	}
	if stats.Min != int64(1) {
		t.Fatalf("Min = %v, want int64(1)", stats.Min)
	}
}

// TestMemoryStoreNodePropertyStatsRescanSkipsOrphanedLabelIndexEntry mirrors
// the badger regression: a label-index entry whose node row is gone must be
// skipped by the rescan, not surfaced as an error or a nil-pointer panic.
func TestMemoryStoreNodePropertyStatsRescanSkipsOrphanedLabelIndexEntry(t *testing.T) {
	t.Parallel()
	ms := New()
	t.Cleanup(func() { _ = ms.Close() })

	survivor := types.NewNode(types.NodeID(snowflake.ID(701)), 1, nil)
	if err := survivor.SetProperty("v", int64(3)); err != nil {
		t.Fatalf("SetProperty survivor: %v", err)
	}
	if err := ms.PutNode(survivor); err != nil {
		t.Fatalf("PutNode survivor: %v", err)
	}
	extremum := types.NewNode(types.NodeID(snowflake.ID(702)), 1, nil)
	if err := extremum.SetProperty("v", int64(99)); err != nil {
		t.Fatalf("SetProperty extremum: %v", err)
	}
	if err := ms.PutNode(extremum); err != nil {
		t.Fatalf("PutNode extremum: %v", err)
	}
	if err := ms.DeleteNode(extremum.ID()); err != nil {
		t.Fatalf("DeleteNode extremum: %v", err)
	}

	// Inject an orphan label-index entry pointing at a node ID that was
	// never created.
	ms.mu.Lock()
	ms.labelIdx[1][types.NodeID(snowflake.ID(999))] = struct{}{}
	ms.mu.Unlock()

	stats, err := ms.NodePropertyStats(1, "v")
	if err != nil {
		t.Fatalf("NodePropertyStats with orphaned label-index entry: %v", err)
	}
	if stats.Min != int64(3) || stats.Max != int64(3) {
		t.Fatalf("Min/Max = %v/%v, want both int64(3) (orphan entry must be skipped, not surfaced as an error)", stats.Min, stats.Max)
	}
}

func TestMemoryStoreNodePropertyStatsClearResets(t *testing.T) {
	t.Parallel()
	ms := New()
	t.Cleanup(func() { _ = ms.Close() })

	n := types.NewNode(types.NodeID(snowflake.ID(601)), 1, nil)
	if err := n.SetProperty("v", int64(7)); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := ms.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	stats, err := ms.NodePropertyStats(1, "v")
	if err != nil {
		t.Fatalf("NodePropertyStats after Clear: %v", err)
	}
	if stats != (storecontract.PropertyStats{}) {
		t.Fatalf("stats after Clear = %+v, want zero value", stats)
	}
}

// TestMemoryStoreNodePropertyStatsConcurrent mirrors the badger regression
// (concurrent PutNode/DeleteNode alongside concurrent NodePropertyStats
// reads, under -race): the memory backend's single-lock design has no
// deadlock risk, but this pins that a future refactor cannot introduce a
// re-entrant lock without a race/deadlock catching it.
func TestMemoryStoreNodePropertyStatsConcurrent(t *testing.T) {
	ms := New()
	t.Cleanup(func() { _ = ms.Close() })
	const labelToken = uint16(1)
	const workers = 8
	const opsPerWorker = 50

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				id := int64(worker*opsPerWorker + i + 1)
				n := types.NewNode(types.NodeID(snowflake.ID(id)), labelToken, nil)
				if err := n.SetProperty("v", int64(i)); err != nil {
					t.Errorf("SetProperty: %v", err)
					return
				}
				if err := ms.PutNode(n); err != nil {
					t.Errorf("PutNode: %v", err)
					return
				}
				if i%2 == 0 {
					if err := ms.DeleteNode(n.ID()); err != nil {
						t.Errorf("DeleteNode: %v", err)
						return
					}
				}
			}
		}(w)
	}
	for r := 0; r < workers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				if _, err := ms.NodePropertyStats(labelToken, "v"); err != nil {
					t.Errorf("NodePropertyStats: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	want := sequentialPropertyStatsGroundTruth(workers, opsPerWorker)

	// Arm a deterministic dirty rescan for the final read: add then delete a
	// node holding the (known) live max, so Forget(max) marks the accumulator
	// dirty and the final read recomputes exact Min/Max over the live set.
	temp := types.NewNode(types.NodeID(snowflake.ID(workers*opsPerWorker+1)), labelToken, nil)
	if err := temp.SetProperty("v", want.survivingMax); err != nil {
		t.Fatalf("SetProperty temp: %v", err)
	}
	if err := ms.PutNode(temp); err != nil {
		t.Fatalf("PutNode temp: %v", err)
	}
	if err := ms.DeleteNode(temp.ID()); err != nil {
		t.Fatalf("DeleteNode temp: %v", err)
	}

	got, err := ms.NodePropertyStats(labelToken, "v")
	if err != nil {
		t.Fatalf("final NodePropertyStats: %v", err)
	}
	assertPropertyStatsMatchGroundTruth(t, got, want)
}

// propertyStatsGroundTruth is the sequentially-computed expected state of the
// concurrent-storm workload (see sequentialPropertyStatsGroundTruth): the
// surviving-node Min/Max/Count are exact, everDistinct is the count of DISTINCT
// values EVER observed (NDV never decrements on delete, so the HyperLogLog
// estimate tracks the ever-observed cardinality, not the surviving one).
type propertyStatsGroundTruth struct {
	survivingMin, survivingMax int64
	survivingCount             int
	everDistinct               int
}

// sequentialPropertyStatsGroundTruth replays the concurrent test's op sequence
// SEQUENTIALLY (each worker touches a disjoint id range and only ever
// put-then-maybe-deletes its own id, so the final surviving set is
// interleaving-independent) and returns the exact expected statistics.
func sequentialPropertyStatsGroundTruth(workers, opsPerWorker int) propertyStatsGroundTruth {
	live := make(map[int64]int64)
	ever := make(map[int64]struct{})
	for w := 0; w < workers; w++ {
		for i := 0; i < opsPerWorker; i++ {
			id := int64(w*opsPerWorker + i + 1)
			live[id] = int64(i)
			ever[int64(i)] = struct{}{}
			if i%2 == 0 {
				delete(live, id)
			}
		}
	}
	gt := propertyStatsGroundTruth{survivingCount: len(live), everDistinct: len(ever)}
	first := true
	for _, v := range live {
		if first || v < gt.survivingMin {
			gt.survivingMin = v
		}
		if first || v > gt.survivingMax {
			gt.survivingMax = v
		}
		first = false
	}
	return gt
}

func assertPropertyStatsMatchGroundTruth(t *testing.T, got storecontract.PropertyStats, want propertyStatsGroundTruth) {
	t.Helper()
	if got.Count != int64(want.survivingCount) {
		t.Fatalf("Count = %d, want %d (surviving live nodes)", got.Count, want.survivingCount)
	}
	if got.Min != want.survivingMin {
		t.Fatalf("Min = %v, want int64(%d)", got.Min, want.survivingMin)
	}
	if got.Max != want.survivingMax {
		t.Fatalf("Max = %v, want int64(%d)", got.Max, want.survivingMax)
	}
	// NDV is a HyperLogLog estimate over EVERY value ever observed (delete does
	// not decrement it). Its accuracy is only pinned at large cardinalities
	// (see the sketch's own regression) — at this tiny scale it can be well off
	// — and NDV is NOT what the stale-rescan-overwrite defect corrupts, so this
	// is only a coarse sanity band: positive, and not wildly above the true
	// ever-observed distinct count.
	if got.NDV < 1 || got.NDV > int64(want.everDistinct)+8 {
		t.Fatalf("NDV = %d, want a positive estimate not far above ever-observed distinct %d", got.NDV, want.everDistinct)
	}
}

// TestMemoryStoreNodePropertyStatsSketch mirrors the badger regression: the
// store-internal NodePropertyStatsSketch accessor (used by the tiered
// backend's cross-shard fold) must agree EXACTLY with NodePropertyStats'
// Min/Max/Count, and the returned sketch's own Estimate() must equal
// NodePropertyStats' NDV.
func TestMemoryStoreNodePropertyStatsSketch(t *testing.T) {
	t.Parallel()
	ms := New()
	t.Cleanup(func() { _ = ms.Close() })

	n1 := types.NewNode(types.NodeID(snowflake.ID(1101)), 1, nil)
	if err := n1.SetProperty("score", int64(10)); err != nil {
		t.Fatalf("SetProperty n1: %v", err)
	}
	if err := ms.PutNode(n1); err != nil {
		t.Fatalf("PutNode n1: %v", err)
	}
	n2 := types.NewNode(types.NodeID(snowflake.ID(1102)), 1, nil)
	if err := n2.SetProperty("score", int64(30)); err != nil {
		t.Fatalf("SetProperty n2: %v", err)
	}
	if err := ms.PutNode(n2); err != nil {
		t.Fatalf("PutNode n2: %v", err)
	}

	want, err := ms.NodePropertyStats(1, "score")
	if err != nil {
		t.Fatalf("NodePropertyStats: %v", err)
	}

	sketch, min, max, count, err := ms.NodePropertyStatsSketch(1, "score")
	if err != nil {
		t.Fatalf("NodePropertyStatsSketch: %v", err)
	}
	if sketch == nil {
		t.Fatal("sketch = nil, want a populated sketch")
	}
	if sketch.Precision() != indexpkg.DefaultHLLPrecision {
		t.Fatalf("sketch precision = %d, want %d", sketch.Precision(), indexpkg.DefaultHLLPrecision)
	}
	if sketch.Estimate() != want.NDV {
		t.Fatalf("sketch.Estimate() = %d, want %d (must match NodePropertyStats.NDV)", sketch.Estimate(), want.NDV)
	}
	if min != want.Min || max != want.Max {
		t.Fatalf("sketch min/max = %v/%v, want %v/%v", min, max, want.Min, want.Max)
	}
	if count != want.Count {
		t.Fatalf("sketch count = %d, want %d", count, want.Count)
	}
}

// TestMemoryStoreNodePropertyStatsSketchMissingPairReturnsNil mirrors
// NodePropertyStats' "unregistered → zero value, not an error" convention.
func TestMemoryStoreNodePropertyStatsSketchMissingPairReturnsNil(t *testing.T) {
	t.Parallel()
	ms := New()
	t.Cleanup(func() { _ = ms.Close() })

	sketch, min, max, count, err := ms.NodePropertyStatsSketch(99, "nope")
	if err != nil {
		t.Fatalf("NodePropertyStatsSketch missing pair: %v", err)
	}
	if sketch != nil || min != nil || max != nil || count != 0 {
		t.Fatalf("NodePropertyStatsSketch missing pair = (%v, %v, %v, %d), want all zero", sketch, min, max, count)
	}
}

// TestMemoryStoreNodePropertyStatsSketchIsIndependentClone verifies the
// returned sketch is a CLONE: mutating it must not perturb the store's own
// accumulator.
func TestMemoryStoreNodePropertyStatsSketchIsIndependentClone(t *testing.T) {
	t.Parallel()
	ms := New()
	t.Cleanup(func() { _ = ms.Close() })

	n := types.NewNode(types.NodeID(snowflake.ID(1201)), 1, nil)
	if err := n.SetProperty("v", int64(1)); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	sketch, _, _, _, err := ms.NodePropertyStatsSketch(1, "v")
	if err != nil {
		t.Fatalf("NodePropertyStatsSketch: %v", err)
	}
	before, err := ms.NodePropertyStats(1, "v")
	if err != nil {
		t.Fatalf("NodePropertyStats before mutation: %v", err)
	}

	other, err := indexpkg.NewHyperLogLog(indexpkg.DefaultHLLPrecision)
	if err != nil {
		t.Fatalf("NewHyperLogLog: %v", err)
	}
	other.AddString("some-far-away-value")
	if err := sketch.Merge(other); err != nil {
		t.Fatalf("Merge into the returned clone: %v", err)
	}

	after, err := ms.NodePropertyStats(1, "v")
	if err != nil {
		t.Fatalf("NodePropertyStats after mutating the clone: %v", err)
	}
	if after.NDV != before.NDV {
		t.Fatalf("store NDV changed after mutating the returned clone: before=%d after=%d — the sketch was NOT an independent clone", before.NDV, after.NDV)
	}
}

func TestMemoryStoreNodePropertyStatsErrors(t *testing.T) {
	t.Parallel()

	var nilStore *Store
	if _, err := nilStore.NodePropertyStats(1, "v"); !errors.Is(err, ErrNilStore) {
		t.Fatalf("nil store = %v, want ErrNilStore", err)
	}

	ms := New()
	if _, err := ms.NodePropertyStats(0, "v"); err == nil {
		t.Fatal("token 0 should be rejected")
	}
	if _, err := ms.NodePropertyStats(1, "tkg_version"); err == nil {
		t.Fatal("shadow property key should be rejected")
	}
	if err := ms.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := ms.NodePropertyStats(1, "v"); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("closed store = %v, want ErrStoreClosed", err)
	}
}

func TestMemoryStoreNodePropertyStatsSketchErrors(t *testing.T) {
	t.Parallel()

	var nilStore *Store
	if _, _, _, _, err := nilStore.NodePropertyStatsSketch(1, "v"); !errors.Is(err, ErrNilStore) {
		t.Fatalf("nil store = %v, want ErrNilStore", err)
	}

	ms := New()
	if _, _, _, _, err := ms.NodePropertyStatsSketch(0, "v"); err == nil {
		t.Fatal("token 0 should be rejected")
	}
	if _, _, _, _, err := ms.NodePropertyStatsSketch(1, "tkg_version"); err == nil {
		t.Fatal("shadow property key should be rejected")
	}
	if err := ms.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, _, _, _, err := ms.NodePropertyStatsSketch(1, "v"); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("closed store = %v, want ErrStoreClosed", err)
	}
}
