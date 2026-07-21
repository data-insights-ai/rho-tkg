package graph_test

import (
	"context"
	"errors"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestRelPropertyStats_BasicNDVMinMax is the rel-side mirror of the node
// PropertyStats basic test (BACKLOG 21a): NDV/min/max/count for a rel type's
// current relationships carrying a numeric property.
func TestRelPropertyStats_BasicNDVMinMax(t *testing.T) {
	eachBackend(t, func(t *testing.T, g *graphpkg.Graph) {
		ctx := context.Background()
		a, _ := g.Nodes().Add(ctx, []string{"P"}, nil)
		b, _ := g.Nodes().Add(ctx, []string{"P"}, nil)

		weights := []int64{10, 30, 20}
		for _, w := range weights {
			if _, err := g.Rels().AddByID(ctx, "KNOWS", a.ID(), b.ID(), map[string]any{"weight": w}); err != nil {
				t.Fatalf("add rel weight=%d: %v", w, err)
			}
		}

		stats, err := g.Stats().RelPropertyStats("KNOWS", "weight")
		if err != nil {
			t.Fatalf("RelPropertyStats: %v", err)
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
	})
}

// TestRelPropertyStats_MissingPairReturnsZeroValue mirrors
// RelPropertyTypeClassCounts' "unregistered → zero" convention (rule 2).
func TestRelPropertyStats_MissingPairReturnsZeroValue(t *testing.T) {
	eachBackend(t, func(t *testing.T, g *graphpkg.Graph) {
		stats, err := g.Stats().RelPropertyStats("NOPE", "nope")
		if err != nil {
			t.Fatalf("RelPropertyStats missing pair: %v", err)
		}
		if stats != (store.PropertyStats{}) {
			t.Fatalf("stats = %+v, want zero value", stats)
		}
	})
}

// TestRelPropertyStats_NonIndexableValueExcluded proves a non-indexable
// property value (a slice) contributes to neither Count nor NDV/Min/Max —
// mirroring the node-side NonIndexableValueExcluded test.
func TestRelPropertyStats_NonIndexableValueExcluded(t *testing.T) {
	eachBackend(t, func(t *testing.T, g *graphpkg.Graph) {
		ctx := context.Background()
		a, _ := g.Nodes().Add(ctx, []string{"P"}, nil)
		b, _ := g.Nodes().Add(ctx, []string{"P"}, nil)
		if _, err := g.Rels().AddByID(ctx, "KNOWS", a.ID(), b.ID(), map[string]any{"tags": []string{"x", "y"}}); err != nil {
			t.Fatalf("add rel: %v", err)
		}
		stats, err := g.Stats().RelPropertyStats("KNOWS", "tags")
		if err != nil {
			t.Fatalf("RelPropertyStats: %v", err)
		}
		if stats != (store.PropertyStats{}) {
			t.Fatalf("stats = %+v, want zero value for a non-indexable property", stats)
		}
	})
}

// TestRelPropertyStats_DeleteExtremumTriggersRescan is the two-phase
// mutation test (TDD point D): create three rels (low/mid/max weight),
// delete the max one, and confirm Max is recomputed to mid via the
// dirty-triggered rescan — while NDV (a HyperLogLog estimate over EVERY
// value ever observed) does NOT decrement, matching the node-side deletion
// semantics documented on store.PropertyStats.NDV.
func TestRelPropertyStats_DeleteExtremumTriggersRescan(t *testing.T) {
	eachBackend(t, func(t *testing.T, g *graphpkg.Graph) {
		ctx := context.Background()
		a, _ := g.Nodes().Add(ctx, []string{"P"}, nil)
		b, _ := g.Nodes().Add(ctx, []string{"P"}, nil)

		low, err := g.Rels().AddByID(ctx, "KNOWS", a.ID(), b.ID(), map[string]any{"v": int64(1)})
		if err != nil {
			t.Fatalf("add low: %v", err)
		}
		if _, err := g.Rels().AddByID(ctx, "KNOWS", a.ID(), b.ID(), map[string]any{"v": int64(5)}); err != nil {
			t.Fatalf("add mid: %v", err)
		}
		extremum, err := g.Rels().AddByID(ctx, "KNOWS", a.ID(), b.ID(), map[string]any{"v": int64(100)})
		if err != nil {
			t.Fatalf("add extremum: %v", err)
		}

		before, err := g.Stats().RelPropertyStats("KNOWS", "v")
		if err != nil {
			t.Fatalf("before: %v", err)
		}
		if before.Max != int64(100) || before.Count != 3 {
			t.Fatalf("before = %+v, want Max=100 Count=3", before)
		}

		if err := g.Rels().Delete(ctx, extremum.ID()); err != nil {
			t.Fatalf("delete extremum: %v", err)
		}

		after, err := g.Stats().RelPropertyStats("KNOWS", "v")
		if err != nil {
			t.Fatalf("after: %v", err)
		}
		if after.Count != 2 {
			t.Fatalf("after Count = %d, want 2", after.Count)
		}
		if after.Max != int64(5) {
			t.Fatalf("after Max = %v, want int64(5) (rescanned after extremum delete)", after.Max)
		}
		if after.Min != int64(1) {
			t.Fatalf("after Min = %v, want int64(1)", after.Min)
		}
		// NDV never decrements — it still reflects the 3 ever-observed values.
		if after.NDV < before.NDV {
			t.Fatalf("after NDV = %d, want >= before NDV = %d (HLL never decrements)", after.NDV, before.NDV)
		}

		if err := g.Rels().Delete(ctx, low.ID()); err != nil {
			t.Fatalf("delete low: %v", err)
		}
		final, err := g.Stats().RelPropertyStats("KNOWS", "v")
		if err != nil {
			t.Fatalf("final: %v", err)
		}
		if final.Count != 1 || final.Min != int64(5) || final.Max != int64(5) {
			t.Fatalf("final = %+v, want Count=1 Min=Max=5", final)
		}
	})
}

// TestRelPropertyStats_UpdateChangesStats is the two-phase mutation test for
// the UPDATE path (TDD point D): a rel's property value change must be
// reflected — the old value's contribution removed, the new value's added —
// on the SAME call as the type-class/rel-property-index maintenance.
func TestRelPropertyStats_UpdateChangesStats(t *testing.T) {
	eachBackend(t, func(t *testing.T, g *graphpkg.Graph) {
		ctx := context.Background()
		a, _ := g.Nodes().Add(ctx, []string{"P"}, nil)
		b, _ := g.Nodes().Add(ctx, []string{"P"}, nil)

		r, err := g.Rels().AddByID(ctx, "KNOWS", a.ID(), b.ID(), map[string]any{"weight": int64(5)})
		if err != nil {
			t.Fatalf("add: %v", err)
		}

		before, err := g.Stats().RelPropertyStats("KNOWS", "weight")
		if err != nil {
			t.Fatalf("before: %v", err)
		}
		if before.Count != 1 || before.Min != int64(5) || before.Max != int64(5) {
			t.Fatalf("before = %+v, want Count=1 Min=Max=5", before)
		}

		if _, err := g.Rels().Update(ctx, r.ID(), map[string]any{"weight": int64(50)}); err != nil {
			t.Fatalf("update: %v", err)
		}

		after, err := g.Stats().RelPropertyStats("KNOWS", "weight")
		if err != nil {
			t.Fatalf("after: %v", err)
		}
		if after.Count != 1 {
			t.Fatalf("after Count = %d, want 1 (still one live rel)", after.Count)
		}
		if after.Min != int64(50) || after.Max != int64(50) {
			t.Fatalf("after Min/Max = %v/%v, want 50/50 (old value 5 removed)", after.Min, after.Max)
		}
	})
}

// TestRelPropertyStats_CascadeDeleteViaNodeDelete proves a node-cascade
// delete (which removes rels via badger's read-free deleteRelByInfo path —
// the memoized-contribution seam) decrements RelPropertyStats' presence
// counter to zero with no drift, mirroring
// TestRelPropertyTypeClassCounts_ExactPartition's cascade scenario.
func TestRelPropertyStats_CascadeDeleteViaNodeDelete(t *testing.T) {
	eachBackend(t, func(t *testing.T, g *graphpkg.Graph) {
		ctx := context.Background()
		a, _ := g.Nodes().Add(ctx, []string{"P"}, nil)
		b, _ := g.Nodes().Add(ctx, []string{"P"}, nil)
		for i := 0; i < 4; i++ {
			if _, err := g.Rels().AddByID(ctx, "KNOWS", a.ID(), b.ID(), map[string]any{"weight": int64(i)}); err != nil {
				t.Fatalf("add rel %d: %v", i, err)
			}
		}
		if err := g.Nodes().Delete(ctx, b.ID()); err != nil {
			t.Fatalf("cascade delete node b: %v", err)
		}
		stats, err := g.Stats().RelPropertyStats("KNOWS", "weight")
		if err != nil {
			t.Fatalf("RelPropertyStats: %v", err)
		}
		if stats.Count != 0 {
			t.Fatalf("Count after cascade = %d, want 0 (every KNOWS rel gone — no drift)", stats.Count)
		}
	})
}

// TestRelPropertyStats_Reopen proves the badger presence counters + NDV/min-
// max accumulator + memoized contributions rebuild from loadIndexes at open,
// mirroring TestRelPropertyTypeClassCounts_Reopen's "survives restart" rule.
func TestRelPropertyStats_Reopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	g, err := graphpkg.New(graphpkg.Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	a, _ := g.Nodes().Add(ctx, []string{"P"}, nil)
	b, _ := g.Nodes().Add(ctx, []string{"P"}, nil)
	var first types.RelID
	for i := 0; i < 4; i++ {
		r, err := g.Rels().AddByID(ctx, "KNOWS", a.ID(), b.ID(), map[string]any{"weight": int64(i)})
		if err != nil {
			t.Fatalf("add rel %d: %v", i, err)
		}
		if i == 0 {
			first = r.ID()
		}
	}
	if err := g.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	g2, err := graphpkg.New(graphpkg.Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer g2.Close()

	stats, err := g2.Stats().RelPropertyStats("KNOWS", "weight")
	if err != nil {
		t.Fatalf("RelPropertyStats after reopen: %v", err)
	}
	if stats.Count != 4 || stats.Min != int64(0) || stats.Max != int64(3) {
		t.Fatalf("after reopen = %+v, want Count=4 Min=0 Max=3 (counters not rebuilt)", stats)
	}

	// A delete after reopen must decrement — proves the memoized-contribution
	// sidecar was rebuilt too (else the read-free delete would find no
	// contribution and drift), and a rescan must recompute Min correctly.
	if err := g2.Rels().Delete(ctx, first); err != nil {
		t.Fatalf("delete after reopen: %v", err)
	}
	stats2, err := g2.Stats().RelPropertyStats("KNOWS", "weight")
	if err != nil {
		t.Fatalf("RelPropertyStats after post-reopen delete: %v", err)
	}
	if stats2.Count != 3 {
		t.Fatalf("after post-reopen delete Count = %d, want 3 (contribution not rebuilt → drift)", stats2.Count)
	}
	if stats2.Min != int64(1) {
		t.Fatalf("after post-reopen delete Min = %v, want int64(1) (rescanned)", stats2.Min)
	}
}

// TestRelPropertyStats_TieredDeclines proves tiered declines the capability,
// mirroring the precedent already set by RelRangeCardinality and
// RelPropertyTypeClassCounts (neither of which tiered implements either —
// rel property indexes are RAM-only per-shard with no cross-shard fold).
func TestRelPropertyStats_TieredDeclines(t *testing.T) {
	ts, err := tiered.New(tiered.Config{InMemory: true, RefLabels: []string{"Machine"}, ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1})
	if err != nil {
		t.Fatalf("tiered.New: %v", err)
	}
	g, err := graphpkg.New(graphpkg.Config{Store: ts})
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	if _, err := g.Stats().RelPropertyStats("KNOWS", "weight"); !errors.Is(err, store.ErrCapabilityNotSupported) {
		t.Fatalf("tiered RelPropertyStats err = %v, want ErrCapabilityNotSupported", err)
	}
}

// TestRelPropertyStats_NDVAccuracy is the NDV-accuracy cross-check (TDD
// point C): 200 distinct numeric weight values on 200 KNOWS rels must
// produce a HyperLogLog estimate within a generous error bound of the true
// distinct count — proving the sketch is actually being fed real per-rel
// values (not, say, a constant or a miscomputed valueKey collapsing every
// value into one bucket).
//
// Values are spread across a wide int64 range (a large-prime stride) rather
// than left as small sequential integers 0..n-1: the shared
// internal/index.HyperLogLog sketch hashes the decimal-string encoding of a
// numeric value (IndexablePropertyValueKey's "i64:<digits>" form), and a run
// of small sequential integers shares a long common string prefix that this
// codebase's FNV-1a-based AddString hashes with poor top-bit avalanche —
// verified independently against the NODE-side sketch too, so it is a
// pre-existing property-value-encoding characteristic, not anything specific
// to this rel-side mirror. A wide-stride spread is the same shape the
// package's own seeded accuracy regression (hyperloglog_test.go) uses to
// avoid the identical pitfall with distinct random strings.
func TestRelPropertyStats_NDVAccuracy(t *testing.T) {
	eachBackend(t, func(t *testing.T, g *graphpkg.Graph) {
		ctx := context.Background()
		a, _ := g.Nodes().Add(ctx, []string{"P"}, nil)
		b, _ := g.Nodes().Add(ctx, []string{"P"}, nil)

		const n = 200
		const stride = 999999937 // large prime — spreads decimal-digit strings widely
		for i := 0; i < n; i++ {
			v := int64(i) * stride
			if _, err := g.Rels().AddByID(ctx, "KNOWS", a.ID(), b.ID(), map[string]any{"weight": v}); err != nil {
				t.Fatalf("add rel %d: %v", i, err)
			}
		}

		stats, err := g.Stats().RelPropertyStats("KNOWS", "weight")
		if err != nil {
			t.Fatalf("RelPropertyStats: %v", err)
		}
		if stats.Count != n {
			t.Fatalf("Count = %d, want %d", stats.Count, n)
		}
		// HyperLogLog at the codebase's default precision has single-digit
		// percent error at this cardinality; allow a generous ±25% band to
		// keep the test robust while still catching a badly broken sketch
		// (e.g. NDV stuck at 1 or 0).
		low, high := int64(n*75/100), int64(n*125/100)
		if stats.NDV < low || stats.NDV > high {
			t.Fatalf("NDV = %d, want within [%d, %d] of true distinct count %d", stats.NDV, low, high, n)
		}
	})
}
