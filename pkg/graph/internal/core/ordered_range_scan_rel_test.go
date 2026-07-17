package core

import (
	"context"
	"errors"
	"math"
	"sort"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Rel ordered-range scan (ForEachByTypePropertyRangeOrdered) — the relationship
// mirror of the node ordered / top-k door (Testing Rule 2). Rel property indexes
// are RAM-only, so only memory + badger-RAM backends exist (no badger-disk arm).

func relOrderedBackends() []orderedBackend {
	return []orderedBackend{
		{name: "memory", cfg: Config{Store: memory.New(), SnowflakeNodeID: 0}},
		{name: "badger-ram", cfg: Config{BadgerInMemory: true, SnowflakeNodeID: 1}},
	}
}

func relNumericValueOf(t *testing.T, r *types.Relationship, key string) float64 {
	t.Helper()
	v, ok := r.PropertiesMap()[key]
	if !ok {
		t.Fatalf("rel %d missing property %q", r.ID().SnowflakeID(), key)
	}
	switch x := v.(type) {
	case int:
		return float64(x)
	case int64:
		return float64(x)
	case float64:
		return x
	case float32:
		return float64(x)
	default:
		t.Fatalf("rel %d property %q not numeric: %T", r.ID().SnowflakeID(), key, v)
		return 0
	}
}

// TestRelOrderedRangeScan_ValueOrderContract asserts exact value order (asc +
// desc), ties broken by rel ID ascending in BOTH directions, negatives, and
// mixed int64/float64 buckets — the rel mirror of the node contract test.
func TestRelOrderedRangeScan_ValueOrderContract(t *testing.T) {
	t.Parallel()
	const typeName, key = "SCORED", "w"
	ctx := context.Background()

	for _, be := range relOrderedBackends() {
		be := be
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			g, err := New(be.cfg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { _ = g.Close() })

			a, err := g.Nodes.Add(ctx, []string{"N"}, nil)
			if err != nil {
				t.Fatalf("add a: %v", err)
			}
			b, err := g.Nodes.Add(ctx, []string{"N"}, nil)
			if err != nil {
				t.Fatalf("add b: %v", err)
			}
			if err := g.Index.CreateRelProperty(typeName, key); err != nil {
				t.Fatalf("CreateRelProperty: %v", err)
			}

			specs := []struct {
				val   any
				asF64 float64
			}{
				{val: 5, asF64: 5},
				{val: 5.0, asF64: 5}, // ties with int 5
				{val: -2.0, asF64: -2},
				{val: int64(-2), asF64: -2}, // ties with -2.0
				{val: 0.0, asF64: 0},
				{val: 3.5, asF64: 3.5},
				{val: -7, asF64: -7},
			}
			type vp struct {
				id  types.RelID
				val float64
			}
			var pairs []vp
			for _, s := range specs {
				r, err := g.Rels.AddByID(ctx, typeName, a.ID(), b.ID(), map[string]any{key: s.val})
				if err != nil {
					t.Fatalf("add rel: %v", err)
				}
				pairs = append(pairs, vp{r.ID(), s.asF64})
			}

			collect := func(desc bool) []vp {
				var got []vp
				err := g.Rels.ForEachByTypePropertyRangeOrdered(typeName, key, math.Inf(-1), math.Inf(1), true, true, desc, storepkg.QueryOpts{}, func(r *types.Relationship) bool {
					got = append(got, vp{r.ID(), relNumericValueOf(t, r, key)})
					return true
				})
				if err != nil {
					t.Fatalf("ordered scan (desc=%v): %v", desc, err)
				}
				return got
			}

			assertOrder := func(desc bool) {
				got := collect(desc)
				want := append([]vp(nil), pairs...)
				sort.SliceStable(want, func(i, j int) bool {
					if want[i].val != want[j].val {
						if desc {
							return want[i].val > want[j].val
						}
						return want[i].val < want[j].val
					}
					return want[i].id.SnowflakeID() < want[j].id.SnowflakeID()
				})
				if len(got) != len(want) {
					t.Fatalf("desc=%v: got %d rows, want %d", desc, len(got), len(want))
				}
				for i := range want {
					if got[i].id != want[i].id || got[i].val != want[i].val {
						t.Fatalf("desc=%v row %d: got (id=%d,v=%v) want (id=%d,v=%v)",
							desc, i, got[i].id.SnowflakeID(), got[i].val,
							want[i].id.SnowflakeID(), want[i].val)
					}
				}
				for i := 1; i < len(got); i++ {
					if got[i].val == got[i-1].val && got[i].id.SnowflakeID() <= got[i-1].id.SnowflakeID() {
						t.Fatalf("desc=%v: tie at %v not rel-ID-ascending: %d then %d",
							desc, got[i].val, got[i-1].id.SnowflakeID(), got[i].id.SnowflakeID())
					}
				}
			}
			assertOrder(false)
			assertOrder(true)
		})
	}
}

// TestRelOrderedRangeScan_LimitPushdownAndBounds asserts the top-k early-stop
// (fn returning false halts the scan) and that a bounded [lo,hi] scan with the
// caller's exact post-filter never emits an out-of-range value.
func TestRelOrderedRangeScan_LimitPushdownAndBounds(t *testing.T) {
	t.Parallel()
	const typeName, key = "SCORED", "w"
	ctx := context.Background()

	for _, be := range relOrderedBackends() {
		be := be
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			g, err := New(be.cfg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { _ = g.Close() })

			a, _ := g.Nodes.Add(ctx, []string{"N"}, nil)
			b, _ := g.Nodes.Add(ctx, []string{"N"}, nil)
			if err := g.Index.CreateRelProperty(typeName, key); err != nil {
				t.Fatalf("CreateRelProperty: %v", err)
			}
			for i := 0; i < 50; i++ {
				if _, err := g.Rels.AddByID(ctx, typeName, a.ID(), b.ID(), map[string]any{key: int64(i)}); err != nil {
					t.Fatalf("add rel %d: %v", i, err)
				}
			}

			// Top-3 ascending: fn stops after 3, so we must see exactly 0,1,2.
			var got []float64
			err = g.Rels.ForEachByTypePropertyRangeOrdered(typeName, key, math.Inf(-1), math.Inf(1), true, true, false, storepkg.QueryOpts{}, func(r *types.Relationship) bool {
				got = append(got, relNumericValueOf(t, r, key))
				return len(got) < 3
			})
			if err != nil {
				t.Fatalf("top-k scan: %v", err)
			}
			if len(got) != 3 || got[0] != 0 || got[1] != 1 || got[2] != 2 {
				t.Fatalf("top-3 asc = %v, want [0 1 2]", got)
			}

			// Bounded [10,20) exclusive-hi: exact post-filter must exclude 20.
			var vals []float64
			err = g.Rels.ForEachByTypePropertyRangeOrdered(typeName, key, 10, 20, true, false, false, storepkg.QueryOpts{}, func(r *types.Relationship) bool {
				v := relNumericValueOf(t, r, key)
				if v >= 10 && v < 20 { // caller's exact re-check (over-select contract)
					vals = append(vals, v)
				}
				return true
			})
			if err != nil {
				t.Fatalf("bounded scan: %v", err)
			}
			if len(vals) != 10 || vals[0] != 10 || vals[9] != 19 {
				t.Fatalf("bounded [10,20) = %v, want 10..19", vals)
			}
		})
	}
}

// TestRelOrderedRangeScan_NoIndex asserts the door declines with ErrIndexNotFound
// when no rel property index exists for (type, propKey).
func TestRelOrderedRangeScan_NoIndex(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	g := newTestGraph(t)
	a, _ := g.Nodes.Add(ctx, []string{"N"}, nil)
	b, _ := g.Nodes.Add(ctx, []string{"N"}, nil)
	if _, err := g.Rels.AddByID(ctx, "SCORED", a.ID(), b.ID(), map[string]any{"w": int64(1)}); err != nil {
		t.Fatalf("add rel: %v", err)
	}
	err := g.Rels.ForEachByTypePropertyRangeOrdered("SCORED", "w", math.Inf(-1), math.Inf(1), true, true, false, storepkg.QueryOpts{}, func(*types.Relationship) bool { return true })
	if !errors.Is(err, storepkg.ErrIndexNotFound) {
		t.Fatalf("no-index err = %v, want ErrIndexNotFound", err)
	}
}

// TestRelOrderedRangeScan_TemporalTwoPhase is the Stage-B sound-full-fold proof
// (rule 15): a rel whose numeric value was in range at t0 but moved out by now is
// correctly INCLUDED when the scan is pinned at t0 and EXCLUDED at now — the
// temporal path resolves value-at-t, not the current value. Needs no index.
func TestRelOrderedRangeScan_TemporalTwoPhase(t *testing.T) {
	t.Parallel()
	const typeName, key = "SCORED", "w"
	g := newTxTimeGraph(t)
	ctx := context.Background()

	a, _ := g.Nodes.Add(ctx, []string{"N"}, nil)
	b, _ := g.Nodes.Add(ctx, []string{"N"}, nil)

	// Phase 1: rel with w=15 (in [10,20)), valid from t=10.
	r, err := g.Rels.AddByID(ctx, typeName, a.ID(), b.ID(), vf(10, map[string]any{key: int64(15)}))
	if err != nil {
		t.Fatalf("add rel: %v", err)
	}
	t0 := r.Temporal().TxFrom

	// Phase 2: update to w=99 (out of [10,20)) at a later valid time.
	if _, err := g.Rels.Update(ctx, r.ID(), vf(60, map[string]any{key: int64(99)})); err != nil {
		t.Fatalf("update rel: %v", err)
	}

	collectAt := func(opts storepkg.QueryOpts) []float64 {
		var got []float64
		err := g.Rels.ForEachByTypePropertyRangeOrdered(typeName, key, 10, 20, true, false, false, opts, func(rl *types.Relationship) bool {
			got = append(got, relNumericValueOf(t, rl, key))
			return true
		})
		if err != nil {
			t.Fatalf("temporal ordered scan: %v", err)
		}
		return got
	}

	// Pinned at t0 (belief before the update): the rel's value-at-t0 is 15 ∈ [10,20) → included.
	atT0 := collectAt(storepkg.QueryOpts{TxPin: t0})
	if len(atT0) != 1 || atT0[0] != 15 {
		t.Fatalf("pinned@t0 = %v, want [15] (value-at-t in range)", atT0)
	}

	// At the current belief the head value is 99 ∉ [10,20) → excluded.
	atNow := collectAt(storepkg.QueryOpts{ValidAt: 70})
	if len(atNow) != 0 {
		t.Fatalf("valid@70 = %v, want [] (head value out of range)", atNow)
	}
}
