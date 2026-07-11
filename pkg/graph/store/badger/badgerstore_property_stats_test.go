package badger

import (
	"errors"
	"sync"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestBadgerStoreNodePropertyStats(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n1 := types.NewNode(types.NodeID(snowflake.ID(101)), 1, nil)
	if err := n1.SetProperty("score", int64(10)); err != nil {
		t.Fatalf("SetProperty n1: %v", err)
	}
	if err := bs.PutNode(n1); err != nil {
		t.Fatalf("PutNode n1: %v", err)
	}
	n2 := types.NewNode(types.NodeID(snowflake.ID(102)), 1, nil)
	if err := n2.SetProperty("score", int64(30)); err != nil {
		t.Fatalf("SetProperty n2: %v", err)
	}
	if err := bs.PutNode(n2); err != nil {
		t.Fatalf("PutNode n2: %v", err)
	}
	n3 := types.NewNode(types.NodeID(snowflake.ID(103)), 1, nil)
	if err := n3.SetProperty("score", int64(20)); err != nil {
		t.Fatalf("SetProperty n3: %v", err)
	}
	if err := bs.PutNode(n3); err != nil {
		t.Fatalf("PutNode n3: %v", err)
	}

	stats, err := bs.NodePropertyStats(1, "score")
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

func TestBadgerStoreNodePropertyStatsMissingPairReturnsZeroValue(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	stats, err := bs.NodePropertyStats(99, "nope")
	if err != nil {
		t.Fatalf("NodePropertyStats missing pair: %v", err)
	}
	if stats != (storecontract.PropertyStats{}) {
		t.Fatalf("stats = %+v, want zero value", stats)
	}
}

func TestBadgerStoreNodePropertyStatsNonIndexableValueExcluded(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(301)), 3, nil)
	if err := n.SetProperty("tags", []string{"a", "b"}); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	stats, err := bs.NodePropertyStats(3, "tags")
	if err != nil {
		t.Fatalf("NodePropertyStats: %v", err)
	}
	if stats != (storecontract.PropertyStats{}) {
		t.Fatalf("stats = %+v, want zero value for a non-indexable property", stats)
	}
}

// TestBadgerStoreNodePropertyStatsDeleteExtremumTriggersRescan is the
// delete-the-extremum two-phase test on the badger backend — the same
// scenario as the memory-store test, since the two backends must agree.
func TestBadgerStoreNodePropertyStatsDeleteExtremumTriggersRescan(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	low := types.NewNode(types.NodeID(snowflake.ID(401)), 1, nil)
	if err := low.SetProperty("v", int64(1)); err != nil {
		t.Fatalf("SetProperty low: %v", err)
	}
	if err := bs.PutNode(low); err != nil {
		t.Fatalf("PutNode low: %v", err)
	}
	mid := types.NewNode(types.NodeID(snowflake.ID(402)), 1, nil)
	if err := mid.SetProperty("v", int64(5)); err != nil {
		t.Fatalf("SetProperty mid: %v", err)
	}
	if err := bs.PutNode(mid); err != nil {
		t.Fatalf("PutNode mid: %v", err)
	}
	extremum := types.NewNode(types.NodeID(snowflake.ID(403)), 1, nil)
	if err := extremum.SetProperty("v", int64(100)); err != nil {
		t.Fatalf("SetProperty extremum: %v", err)
	}
	if err := bs.PutNode(extremum); err != nil {
		t.Fatalf("PutNode extremum: %v", err)
	}

	before, err := bs.NodePropertyStats(1, "v")
	if err != nil {
		t.Fatalf("NodePropertyStats (before): %v", err)
	}
	if before.Max != int64(100) {
		t.Fatalf("Max (before) = %v, want int64(100)", before.Max)
	}

	if err := bs.DeleteNode(extremum.ID()); err != nil {
		t.Fatalf("DeleteNode extremum: %v", err)
	}

	after, err := bs.NodePropertyStats(1, "v")
	if err != nil {
		t.Fatalf("NodePropertyStats (after): %v", err)
	}
	if after.Max != int64(5) {
		t.Fatalf("Max (after delete-the-extremum) = %v, want int64(5) — must NOT still report the deleted 100", after.Max)
	}
	if after.Min != int64(1) {
		t.Fatalf("Min (after) = %v, want int64(1)", after.Min)
	}
	if after.Count != 2 {
		t.Fatalf("Count (after) = %d, want 2", after.Count)
	}
}

// TestBadgerStoreNodePropertyStatsRebuildsOnOpen is the restart-survival
// regression: NDV/min/max/count must be reconstructed from the persisted
// node rows at open, exactly like the sibling presence counter.
func TestBadgerStoreNodePropertyStatsRebuildsOnOpen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	bs1, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("New bs1: %v", err)
	}
	n1 := types.NewNode(types.NodeID(snowflake.ID(501)), 7, nil)
	if err := n1.SetProperty("v", int64(3)); err != nil {
		t.Fatalf("SetProperty n1: %v", err)
	}
	if err := bs1.PutNode(n1); err != nil {
		t.Fatalf("PutNode n1: %v", err)
	}
	n2 := types.NewNode(types.NodeID(snowflake.ID(502)), 7, nil)
	if err := n2.SetProperty("v", int64(30)); err != nil {
		t.Fatalf("SetProperty n2: %v", err)
	}
	if err := bs1.PutNode(n2); err != nil {
		t.Fatalf("PutNode n2: %v", err)
	}
	if err := bs1.Flush(); err != nil {
		t.Fatalf("Flush bs1: %v", err)
	}
	if err := bs1.Close(); err != nil {
		t.Fatalf("Close bs1: %v", err)
	}

	bs2, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("New bs2: %v", err)
	}
	defer bs2.Close()

	stats, err := bs2.NodePropertyStats(7, "v")
	if err != nil {
		t.Fatalf("NodePropertyStats after reopen: %v", err)
	}
	if stats.Count != 2 {
		t.Fatalf("Count after reopen = %d, want 2", stats.Count)
	}
	if stats.Min != int64(3) {
		t.Fatalf("Min after reopen = %v, want int64(3)", stats.Min)
	}
	if stats.Max != int64(30) {
		t.Fatalf("Max after reopen = %v, want int64(30)", stats.Max)
	}
	if stats.NDV < 1 || stats.NDV > 10 {
		t.Fatalf("NDV after reopen = %d, want a small positive estimate (2 distinct values)", stats.NDV)
	}
}

// TestBadgerStoreNodePropertyStatsRescanSkipsOrphanedLabelIndexEntry pins the
// "orphaned label-index entry" tolerance documented on
// collectCurrentPropertyValuesLocked: a label-index key whose node row is
// gone (a crash-window orphan, or here a synthetic one injected directly)
// must be skipped by the rescan, not surfaced as an error.
func TestBadgerStoreNodePropertyStatsRescanSkipsOrphanedLabelIndexEntry(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	survivor := types.NewNode(types.NodeID(snowflake.ID(701)), 1, nil)
	if err := survivor.SetProperty("v", int64(3)); err != nil {
		t.Fatalf("SetProperty survivor: %v", err)
	}
	if err := bs.PutNode(survivor); err != nil {
		t.Fatalf("PutNode survivor: %v", err)
	}
	extremum := types.NewNode(types.NodeID(snowflake.ID(702)), 1, nil)
	if err := extremum.SetProperty("v", int64(99)); err != nil {
		t.Fatalf("SetProperty extremum: %v", err)
	}
	if err := bs.PutNode(extremum); err != nil {
		t.Fatalf("PutNode extremum: %v", err)
	}
	if err := bs.DeleteNode(extremum.ID()); err != nil {
		t.Fatalf("DeleteNode extremum: %v", err)
	}

	// Inject an orphan label-index entry pointing at a node ID that was
	// never created — DeleteNode already cleaned up the real orphan risk
	// (the label index no longer references node 702), so this synthesizes
	// the crash-window scenario directly rather than relying on timing.
	bs.idxMu.Lock()
	bs.labelIdx[1][types.NodeID(snowflake.ID(999))] = struct{}{}
	bs.idxMu.Unlock()

	stats, err := bs.NodePropertyStats(1, "v")
	if err != nil {
		t.Fatalf("NodePropertyStats with orphaned label-index entry: %v", err)
	}
	if stats.Min != int64(3) || stats.Max != int64(3) {
		t.Fatalf("Min/Max = %v/%v, want both int64(3) (orphan entry must be skipped, not surfaced as an error)", stats.Min, stats.Max)
	}
}

// TestBadgerStoreNodePropertyStatsConcurrent drives concurrent
// PutNode/DeleteNode mutations (triggering Observe/Forget/dirty-rescan)
// alongside concurrent NodePropertyStats reads, under -race. It caught a
// real deadlock during development (NodePropertyStats holding idxMu.Lock()
// across a rescan's node-fetch loop, which itself needs idxMu.RLock() on a
// cache-cold node — see the fine-grained-locking doc comment on
// NodePropertyStats) — kept as a permanent regression against reintroducing
// that lock-scope bug. After the storm it asserts VALUE CORRECTNESS: a final
// quiescent read must equal the sequentially-computed ground truth over the
// surviving live nodes — the stale-rescan-overwrite defect (a lost concurrent
// extremum) would leave the exact Min/Max wrong here even though no worker
// errored.
func TestBadgerStoreNodePropertyStatsConcurrent(t *testing.T) {
	bs := newTestBadgerStore(t)
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
				if err := bs.PutNode(n); err != nil {
					t.Errorf("PutNode: %v", err)
					return
				}
				if i%2 == 0 {
					if err := bs.DeleteNode(n.ID()); err != nil {
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
				if _, err := bs.NodePropertyStats(labelToken, "v"); err != nil {
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
	// dirty and the final read recomputes exact Min/Max over the live set —
	// exercising the fixed rescan path against a known-correct answer rather
	// than whatever Observe-only state the storm happened to leave.
	temp := types.NewNode(types.NodeID(snowflake.ID(workers*opsPerWorker+1)), labelToken, nil)
	if err := temp.SetProperty("v", want.survivingMax); err != nil {
		t.Fatalf("SetProperty temp: %v", err)
	}
	if err := bs.PutNode(temp); err != nil {
		t.Fatalf("PutNode temp: %v", err)
	}
	if err := bs.DeleteNode(temp.ID()); err != nil {
		t.Fatalf("DeleteNode temp: %v", err)
	}

	got, err := bs.NodePropertyStats(labelToken, "v")
	if err != nil {
		t.Fatalf("final NodePropertyStats: %v", err)
	}
	assertPropertyStatsMatchGroundTruth(t, got, want)
}

// TestBadgerStoreNodePropertyStatsStaleRescanOverwrite is the deterministic
// reproduction of the stale-rescan-overwrite defect: a concurrent PutNode
// landing a NEW live extremum strictly between the unlocked value collection
// and the Rescan commit must NOT be overwritten by the stale snapshot. The
// rescanTestHook fires exactly once (attempt 0), inside the collect→commit
// window, and lands a node whose "v" is far above anything the collect saw.
// A correct implementation re-collects (the write-generation moved) and
// returns the TRUE live max; the pre-fix implementation committed the stale
// Rescan and returned the old max forever.
func TestBadgerStoreNodePropertyStatsStaleRescanOverwrite(t *testing.T) {
	bs := newTestBadgerStore(t)
	const labelToken = uint16(1)

	// Population: v in {1, 2}, plus an extremum 100 we delete to arm dirty.
	for id, v := range map[int64]int64{801: 1, 802: 2, 803: 100} {
		n := types.NewNode(types.NodeID(snowflake.ID(id)), labelToken, nil)
		if err := n.SetProperty("v", v); err != nil {
			t.Fatalf("SetProperty %d: %v", id, err)
		}
		if err := bs.PutNode(n); err != nil {
			t.Fatalf("PutNode %d: %v", id, err)
		}
	}
	if err := bs.DeleteNode(types.NodeID(snowflake.ID(803))); err != nil {
		t.Fatalf("DeleteNode extremum: %v", err)
	}

	const liveMax = int64(999999)
	var once sync.Once
	bs.rescanTestHook = func(int) {
		// Land a new live extremum inside the collect→commit window, exactly
		// once, so the fixed re-collect terminates on the second attempt.
		once.Do(func() {
			n := types.NewNode(types.NodeID(snowflake.ID(804)), labelToken, nil)
			if err := n.SetProperty("v", liveMax); err != nil {
				t.Errorf("hook SetProperty: %v", err)
				return
			}
			if err := bs.PutNode(n); err != nil {
				t.Errorf("hook PutNode: %v", err)
			}
		})
	}

	stats, err := bs.NodePropertyStats(labelToken, "v")
	if err != nil {
		t.Fatalf("NodePropertyStats: %v", err)
	}
	if stats.Max != liveMax {
		t.Fatalf("Max = %v, want int64(%d) — the concurrent live extremum was overwritten by a stale rescan", stats.Max, liveMax)
	}
	if stats.Min != int64(1) {
		t.Fatalf("Min = %v, want int64(1)", stats.Min)
	}

	// A quiescent follow-up read (hook already spent) must agree: the state is
	// now clean and exact.
	bs.rescanTestHook = nil
	stats2, err := bs.NodePropertyStats(labelToken, "v")
	if err != nil {
		t.Fatalf("NodePropertyStats (follow-up): %v", err)
	}
	if stats2.Max != liveMax || stats2.Min != int64(1) {
		t.Fatalf("follow-up Min/Max = %v/%v, want 1/%d", stats2.Min, stats2.Max, liveMax)
	}
}

// TestBadgerStoreNodePropertyStatsRescanGenerationExhaustion exercises the
// bounded-retry exhaustion fallback: when a concurrent mutation lands in the
// collect→commit window on EVERY attempt, the retry loop gives up after its
// bound and returns the LIVE snapshot (never a stale committed Rescan),
// leaving the accumulator dirty so a later quiescent read reconciles.
func TestBadgerStoreNodePropertyStatsRescanGenerationExhaustion(t *testing.T) {
	bs := newTestBadgerStore(t)
	const labelToken = uint16(1)

	for id, v := range map[int64]int64{811: 1, 812: 2, 813: 100} {
		n := types.NewNode(types.NodeID(snowflake.ID(id)), labelToken, nil)
		if err := n.SetProperty("v", v); err != nil {
			t.Fatalf("SetProperty %d: %v", id, err)
		}
		if err := bs.PutNode(n); err != nil {
			t.Fatalf("PutNode %d: %v", id, err)
		}
	}
	if err := bs.DeleteNode(types.NodeID(snowflake.ID(813))); err != nil {
		t.Fatalf("DeleteNode extremum: %v", err)
	}

	// Land a fresh, ever-higher extremum on every attempt so the write
	// generation always moves and the loop exhausts its bound.
	var landed int64
	nextID := int64(900)
	bs.rescanTestHook = func(int) {
		landed++
		nextID++
		n := types.NewNode(types.NodeID(snowflake.ID(nextID)), labelToken, nil)
		if err := n.SetProperty("v", 1000+landed); err != nil {
			t.Errorf("hook SetProperty: %v", err)
			return
		}
		if err := bs.PutNode(n); err != nil {
			t.Errorf("hook PutNode: %v", err)
		}
	}

	stats, err := bs.NodePropertyStats(labelToken, "v")
	if err != nil {
		t.Fatalf("NodePropertyStats: %v", err)
	}
	if landed != propertyStatsRescanMaxAttempts {
		t.Fatalf("hook fired %d times, want %d (one collect per bounded attempt)", landed, propertyStatsRescanMaxAttempts)
	}
	// Observe on the write path keeps Max monotonically correct for additions,
	// so the exhaustion fallback returns the highest value landed so far — a
	// live value, never the pre-mutation stale one.
	wantMax := int64(1000) + landed
	if stats.Max != wantMax {
		t.Fatalf("Max on exhaustion = %v, want int64(%d) (the live snapshot, not a stale committed rescan)", stats.Max, wantMax)
	}

	// The accumulator was NOT reconciled (no clean Rescan committed): it stays
	// dirty so a later, quiescent read fixes it up.
	bs.idxMu.RLock()
	acc := bs.propertyStats[indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: "v"}]
	stillDirty := acc != nil && acc.Dirty()
	bs.idxMu.RUnlock()
	if !stillDirty {
		t.Fatal("accumulator must remain dirty after exhaustion so a later read reconciles it")
	}

	// Quiescent follow-up: no hook, gen stable → clean Rescan → exact.
	bs.rescanTestHook = nil
	stats2, err := bs.NodePropertyStats(labelToken, "v")
	if err != nil {
		t.Fatalf("NodePropertyStats (follow-up): %v", err)
	}
	if stats2.Max != wantMax {
		t.Fatalf("follow-up Max = %v, want int64(%d)", stats2.Max, wantMax)
	}
}

func TestBadgerStoreNodePropertyStatsErrors(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	if _, err := bs.NodePropertyStats(0, "v"); err == nil {
		t.Fatal("token 0 should be rejected")
	}
	if _, err := bs.NodePropertyStats(1, "tkg_version"); err == nil {
		t.Fatal("shadow property key should be rejected")
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := bs.NodePropertyStats(1, "v"); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("closed store = %v, want ErrStoreClosed", err)
	}
}

func TestBadgerStoreNodePropertyStatsSketchErrors(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	if _, _, _, _, err := bs.NodePropertyStatsSketch(0, "v"); err == nil {
		t.Fatal("token 0 should be rejected")
	}
	if _, _, _, _, err := bs.NodePropertyStatsSketch(1, "tkg_version"); err == nil {
		t.Fatal("shadow property key should be rejected")
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, _, _, _, err := bs.NodePropertyStatsSketch(1, "v"); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("closed store = %v, want ErrStoreClosed", err)
	}
}

func TestBadgerStoreNodePropertyStatsClearResets(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(601)), 1, nil)
	if err := n.SetProperty("v", int64(7)); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := bs.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	stats, err := bs.NodePropertyStats(1, "v")
	if err != nil {
		t.Fatalf("NodePropertyStats after Clear: %v", err)
	}
	if stats != (storecontract.PropertyStats{}) {
		t.Fatalf("stats after Clear = %+v, want zero value", stats)
	}
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

// TestBadgerStoreNodePropertyStatsSketch pins the store-internal
// NodePropertyStatsSketch accessor (not part of the public
// NodePropertyStatsCapability contract, but a direct public method the
// tiered backend's cross-shard fold calls): it must agree EXACTLY with
// NodePropertyStats' Min/Max/Count, and the returned sketch's own
// Estimate() must equal NodePropertyStats' NDV — since NodePropertyStats is
// defined as exactly the sketch's Estimate() plus the accumulator's
// min/max/count, they cannot diverge if both read the same accumulator.
func TestBadgerStoreNodePropertyStatsSketch(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n1 := types.NewNode(types.NodeID(snowflake.ID(1101)), 1, nil)
	if err := n1.SetProperty("score", int64(10)); err != nil {
		t.Fatalf("SetProperty n1: %v", err)
	}
	if err := bs.PutNode(n1); err != nil {
		t.Fatalf("PutNode n1: %v", err)
	}
	n2 := types.NewNode(types.NodeID(snowflake.ID(1102)), 1, nil)
	if err := n2.SetProperty("score", int64(30)); err != nil {
		t.Fatalf("SetProperty n2: %v", err)
	}
	if err := bs.PutNode(n2); err != nil {
		t.Fatalf("PutNode n2: %v", err)
	}

	want, err := bs.NodePropertyStats(1, "score")
	if err != nil {
		t.Fatalf("NodePropertyStats: %v", err)
	}

	sketch, min, max, count, err := bs.NodePropertyStatsSketch(1, "score")
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

// TestBadgerStoreNodePropertyStatsSketchMissingPairReturnsNil mirrors
// NodePropertyStats' "unregistered → zero value, not an error" convention.
func TestBadgerStoreNodePropertyStatsSketchMissingPairReturnsNil(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	sketch, min, max, count, err := bs.NodePropertyStatsSketch(99, "nope")
	if err != nil {
		t.Fatalf("NodePropertyStatsSketch missing pair: %v", err)
	}
	if sketch != nil || min != nil || max != nil || count != 0 {
		t.Fatalf("NodePropertyStatsSketch missing pair = (%v, %v, %v, %d), want all zero", sketch, min, max, count)
	}
}

// TestBadgerStoreNodePropertyStatsSketchIsIndependentClone verifies the
// returned sketch is a CLONE: mutating it (Merge-ing in another sketch) must
// not perturb the store's own accumulator — a shared live pointer would let
// the tiered fold's cross-shard Merge corrupt one shard's internal state.
func TestBadgerStoreNodePropertyStatsSketchIsIndependentClone(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(1201)), 1, nil)
	if err := n.SetProperty("v", int64(1)); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	sketch, _, _, _, err := bs.NodePropertyStatsSketch(1, "v")
	if err != nil {
		t.Fatalf("NodePropertyStatsSketch: %v", err)
	}
	before, err := bs.NodePropertyStats(1, "v")
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

	after, err := bs.NodePropertyStats(1, "v")
	if err != nil {
		t.Fatalf("NodePropertyStats after mutating the clone: %v", err)
	}
	if after.NDV != before.NDV {
		t.Fatalf("store NDV changed after mutating the returned clone: before=%d after=%d — the sketch was NOT an independent clone", before.NDV, after.NDV)
	}
}

func assertPropertyStatsMatchGroundTruth(t *testing.T, got storecontract.PropertyStats, want propertyStatsGroundTruth) {
	t.Helper()
	if got.Count != int64(want.survivingCount) {
		t.Fatalf("Count = %d, want %d (surviving live nodes)", got.Count, want.survivingCount)
	}
	if got.Min != want.survivingMin {
		t.Fatalf("Min = %v, want int64(%d) — a lost concurrent extremum would show here", got.Min, want.survivingMin)
	}
	if got.Max != want.survivingMax {
		t.Fatalf("Max = %v, want int64(%d) — a lost concurrent extremum would show here", got.Max, want.survivingMax)
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
