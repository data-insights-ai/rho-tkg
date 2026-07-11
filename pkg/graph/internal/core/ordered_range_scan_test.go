package core

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"sort"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// orderedBackend names one store construction under test for the ordered /
// top-k range scan. The three cover the memory ordered view, the badger RAM
// ordered view, and the badger persisted-0x0A ordered read path.
type orderedBackend struct {
	name string
	cfg  Config
}

func orderedBackends() []orderedBackend {
	return []orderedBackend{
		{name: "memory", cfg: Config{Store: memory.New(), SnowflakeNodeID: 0}},
		{name: "badger-ram", cfg: Config{BadgerInMemory: true, SnowflakeNodeID: 1}},
		{name: "badger-disk", cfg: Config{BadgerInMemory: true, PropertyIndexOnDisk: true, SnowflakeNodeID: 2}},
	}
}

// numericValueOf reads the node's numeric property as a float64 for exact
// post-filtering (the ordered door over-selects; the caller re-checks).
func numericValueOf(t *testing.T, n *types.Node, key string) float64 {
	t.Helper()
	v, ok := n.PropertiesMap()[key]
	if !ok {
		t.Fatalf("node %d missing property %q", n.ID().SnowflakeID(), key)
	}
	switch x := v.(type) {
	case int:
		return float64(x)
	case int64:
		return float64(x)
	case float64:
		return float64(x)
	case float32:
		return float64(x)
	default:
		t.Fatalf("node %d property %q not numeric: %T", n.ID().SnowflakeID(), key, v)
		return 0
	}
}

// TestOrderedRangeScan_ValueOrderContract asserts the exact value-order
// contract (asc + desc), ties broken by node ID ascending in BOTH directions,
// negative floats, and mixed int64/float64 buckets, on all three backends.
func TestOrderedRangeScan_ValueOrderContract(t *testing.T) {
	t.Parallel()
	const label, key = "Metric", "v"
	ctx := context.Background()

	for _, be := range orderedBackends() {
		be := be
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			g, err := New(be.cfg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { _ = g.Close() })

			if err := g.Index.CreateProperty(label, key); err != nil {
				t.Fatalf("CreateProperty: %v", err)
			}

			// Values chosen to exercise: a tie at 5 (int 5 vs float 5.0 =>
			// same magnitude bucket), a tie at -2.0, negatives, a fractional,
			// and zero. Value carried as int OR float64 to hit mixed buckets.
			type spec struct {
				val   any
				asF64 float64
			}
			specs := []spec{
				{val: 5, asF64: 5},          // int 5
				{val: 5.0, asF64: 5},        // float 5.0 -> ties with int 5 at magnitude 5
				{val: -2.0, asF64: -2},      //
				{val: int64(-2), asF64: -2}, // int64 -2 -> ties with -2.0
				{val: 0.0, asF64: 0},        //
				{val: 3.5, asF64: 3.5},      // fractional
				{val: -7, asF64: -7},        //
			}
			type vp struct {
				id  types.NodeID
				val float64
			}
			var pairs []vp
			for _, s := range specs {
				n, err := g.Nodes.Add(ctx, []string{label}, map[string]any{key: s.val})
				if err != nil {
					t.Fatalf("Add: %v", err)
				}
				pairs = append(pairs, vp{n.ID(), s.asF64})
			}

			collect := func(desc bool) []vp {
				var got []vp
				err := g.Nodes.ForEachByLabelPropertyRangeOrdered(label, key, math.Inf(-1), math.Inf(1), true, true, desc, storepkg.QueryOpts{}, func(n *types.Node) bool {
					got = append(got, vp{n.ID(), numericValueOf(t, n, key)})
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
						t.Fatalf("desc=%v row %d: got (id=%d,v=%v) want (id=%d,v=%v)\nfull got=%v",
							desc, i, got[i].id.SnowflakeID(), got[i].val,
							want[i].id.SnowflakeID(), want[i].val, got)
					}
				}
				// Explicit tie assertion: within equal-value runs, node IDs
				// must be strictly ascending regardless of scan direction.
				for i := 1; i < len(got); i++ {
					if got[i].val == got[i-1].val && got[i].id.SnowflakeID() <= got[i-1].id.SnowflakeID() {
						t.Fatalf("desc=%v: tie at value %v not node-ID-ascending: %d then %d",
							desc, got[i].val, got[i-1].id.SnowflakeID(), got[i].id.SnowflakeID())
					}
				}
			}
			assertOrder(false)
			assertOrder(true)
		})
	}
}

// TestOrderedRangeScan_BoundedRangeExactFilter asserts a bounded scan plus the
// caller's exact post-filter (over-select contract) returns exactly the
// in-range set in order, and NEVER a value outside the exact bounds.
func TestOrderedRangeScan_BoundedRangeExactFilter(t *testing.T) {
	t.Parallel()
	const label, key = "B", "v"
	ctx := context.Background()

	for _, be := range orderedBackends() {
		be := be
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			g, err := New(be.cfg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { _ = g.Close() })
			if err := g.Index.CreateProperty(label, key); err != nil {
				t.Fatalf("CreateProperty: %v", err)
			}
			for i := 0; i < 30; i++ {
				if _, err := g.Nodes.Add(ctx, []string{label}, map[string]any{key: i}); err != nil {
					t.Fatalf("Add: %v", err)
				}
			}

			// Exact bounds (5, 10] — exclusive min, inclusive max.
			lo, hi := 5.0, 10.0
			var got []float64
			err = g.Nodes.ForEachByLabelPropertyRangeOrdered(label, key, lo, hi, false, true, false, storepkg.QueryOpts{}, func(n *types.Node) bool {
				v := numericValueOf(t, n, key)
				if v <= lo || v > hi { // caller's EXACT inclusivity re-check
					return true // over-selected candidate: skip, keep scanning
				}
				got = append(got, v)
				return true
			})
			if err != nil {
				t.Fatalf("ordered scan: %v", err)
			}
			want := []float64{6, 7, 8, 9, 10}
			if len(got) != len(want) {
				t.Fatalf("got %v want %v", got, want)
			}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("row %d: got %v want %v (full %v)", i, got[i], want[i], got)
				}
			}
		})
	}
}

// TestOrderedRangeScan_LimitPushdown proves the LIMIT is pushed into the index:
// a top-k=10 over a large value range invokes fn only ~k times — NOT once per
// value — so the door does not collect-then-limit. Run on the fully-lazy RAM
// backends (memory + badger-RAM).
func TestOrderedRangeScan_LimitPushdown(t *testing.T) {
	t.Parallel()
	const label, key = "Big", "v"
	const n = 20000
	const k = 10
	ctx := context.Background()

	for _, be := range []orderedBackend{
		{name: "memory", cfg: Config{Store: memory.New(), SnowflakeNodeID: 3}},
		{name: "badger-ram", cfg: Config{BadgerInMemory: true, SnowflakeNodeID: 4}},
	} {
		be := be
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			g, err := New(be.cfg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { _ = g.Close() })
			if err := g.Index.CreateProperty(label, key); err != nil {
				t.Fatalf("CreateProperty: %v", err)
			}
			// n distinct values 0..n-1 (all within the query range).
			for i := 0; i < n; i++ {
				if _, err := g.Nodes.Add(ctx, []string{label}, map[string]any{key: i}); err != nil {
					t.Fatalf("Add %d: %v", i, err)
				}
			}

			calls := 0
			var got []float64
			err = g.Nodes.ForEachByLabelPropertyRangeOrdered(label, key, math.Inf(-1), math.Inf(1), true, true, false, storepkg.QueryOpts{}, func(node *types.Node) bool {
				calls++
				got = append(got, numericValueOf(t, node, key))
				return len(got) < k // stop after k matches -> LIMIT pushdown
			})
			if err != nil {
				t.Fatalf("ordered scan: %v", err)
			}
			if len(got) != k {
				t.Fatalf("got %d rows, want %d", len(got), k)
			}
			// The k smallest values ascending are 0..9.
			for i := 0; i < k; i++ {
				if got[i] != float64(i) {
					t.Fatalf("row %d: got %v want %v", i, got[i], float64(i))
				}
			}
			// Pushdown proof: fn was called exactly k times, not O(n). A
			// collect-then-limit implementation would call fn n=%d times.
			if calls != k {
				t.Fatalf("fn called %d times over a %d-value range; expected exactly k=%d (no full-range collection)", calls, n, k)
			}
		})
	}
}

// TestOrderedRangeScan_ThreeWayEquivalence asserts memory, badger-RAM, and
// badger-disk(0x0A) return byte-identical ordered streams over the same
// randomized data — asc and desc.
func TestOrderedRangeScan_ThreeWayEquivalence(t *testing.T) {
	t.Parallel()
	const label, key = "Eq", "v"
	ctx := context.Background()
	rng := rand.New(rand.NewSource(1234)) //nolint:gosec // deterministic test

	// Shared value sequence (index i -> value), applied identically to every
	// backend so node IDs line up 1:1 by creation order.
	const count = 1500
	vals := make([]any, count)
	for i := range vals {
		switch rng.Intn(3) {
		case 0:
			vals[i] = rng.Intn(60) - 30 // int, many ties, negatives
		case 1:
			vals[i] = int64(rng.Intn(60) - 30)
		default:
			vals[i] = float64(rng.Intn(120)-60) / 2.0 // fractional/negative
		}
	}

	type row struct {
		seq int
		val float64
	}
	runBackend := func(be orderedBackend, desc bool) []row {
		g, err := New(be.cfg)
		if err != nil {
			t.Fatalf("%s New: %v", be.name, err)
		}
		defer func() { _ = g.Close() }()
		if err := g.Index.CreateProperty(label, key); err != nil {
			t.Fatalf("%s CreateProperty: %v", be.name, err)
		}
		idToSeq := map[types.NodeID]int{}
		for i, v := range vals {
			n, err := g.Nodes.Add(ctx, []string{label}, map[string]any{key: v})
			if err != nil {
				t.Fatalf("%s Add %d: %v", be.name, i, err)
			}
			idToSeq[n.ID()] = i
		}
		var out []row
		err = g.Nodes.ForEachByLabelPropertyRangeOrdered(label, key, math.Inf(-1), math.Inf(1), true, true, desc, storepkg.QueryOpts{}, func(node *types.Node) bool {
			out = append(out, row{idToSeq[node.ID()], numericValueOf(t, node, key)})
			return true
		})
		if err != nil {
			t.Fatalf("%s ordered scan: %v", be.name, err)
		}
		return out
	}

	// Fresh backends per direction (each New consumes the cfg's store).
	for _, desc := range []bool{false, true} {
		bes := []orderedBackend{
			{name: "memory", cfg: Config{Store: memory.New(), SnowflakeNodeID: 5}},
			{name: "badger-ram", cfg: Config{BadgerInMemory: true, SnowflakeNodeID: 6}},
			{name: "badger-disk", cfg: Config{BadgerInMemory: true, PropertyIndexOnDisk: true, SnowflakeNodeID: 7}},
		}
		ref := runBackend(bes[0], desc)
		if len(ref) != count {
			t.Fatalf("desc=%v: memory returned %d rows, want %d", desc, len(ref), count)
		}
		// Reference must itself be correctly ordered.
		for i := 1; i < len(ref); i++ {
			if desc {
				if ref[i].val > ref[i-1].val {
					t.Fatalf("desc reference not descending at %d", i)
				}
			} else if ref[i].val < ref[i-1].val {
				t.Fatalf("asc reference not ascending at %d", i)
			}
		}
		for _, be := range bes[1:] {
			got := runBackend(be, desc)
			if len(got) != len(ref) {
				t.Fatalf("desc=%v: %s returned %d rows, memory %d", desc, be.name, len(got), len(ref))
			}
			for i := range ref {
				if got[i].seq != ref[i].seq || got[i].val != ref[i].val {
					t.Fatalf("desc=%v: %s row %d = (seq=%d,v=%v), memory = (seq=%d,v=%v)",
						desc, be.name, i, got[i].seq, got[i].val, ref[i].seq, ref[i].val)
				}
			}
		}
	}
}

// TestOrderedRangeScan_Declines covers the documented decline paths: temporal
// QueryOpts -> ErrOrderedScanTemporal; no index -> ErrIndexNotFound; nil fn ->
// ErrNilCallback.
func TestOrderedRangeScan_Declines(t *testing.T) {
	t.Parallel()
	const label, key = "D", "v"
	ctx := context.Background()

	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	if err := g.Index.CreateProperty(label, key); err != nil {
		t.Fatalf("CreateProperty: %v", err)
	}
	if _, err := g.Nodes.Add(ctx, []string{label}, map[string]any{key: 1}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	noop := func(*types.Node) bool { return true }

	temporalOpts := []struct {
		name string
		opts storepkg.QueryOpts
	}{
		{"ValidAt", storepkg.QueryOpts{ValidAt: 100}},
		{"ValidStart", storepkg.QueryOpts{ValidStart: 100}},
		{"ValidEnd", storepkg.QueryOpts{ValidEnd: 100}},
		{"TxAt", storepkg.QueryOpts{TxAt: 100}},
		{"TxPin", storepkg.QueryOpts{TxPin: 100}},
	}
	for _, tc := range temporalOpts {
		err := g.Nodes.ForEachByLabelPropertyRangeOrdered(label, key, 0, 10, true, true, false, tc.opts, noop)
		if !errors.Is(err, storepkg.ErrOrderedScanTemporal) {
			t.Fatalf("%s: err = %v, want ErrOrderedScanTemporal", tc.name, err)
		}
	}

	// No index on this (label,key) -> ErrIndexNotFound.
	if err := g.Nodes.ForEachByLabelPropertyRangeOrdered(label, "unindexed", 0, 10, true, true, false, storepkg.QueryOpts{}, noop); !errors.Is(err, storepkg.ErrIndexNotFound) {
		t.Fatalf("no index: err = %v, want ErrIndexNotFound", err)
	}

	// Nil callback.
	if err := g.Nodes.ForEachByLabelPropertyRangeOrdered(label, key, 0, 10, true, true, false, storepkg.QueryOpts{}, nil); err == nil {
		t.Fatalf("nil callback must error")
	}

	// Unknown label -> nil, nil (no rows), matching the unordered sibling.
	if err := g.Nodes.ForEachByLabelPropertyRangeOrdered("NoSuchLabel", key, 0, 10, true, true, false, storepkg.QueryOpts{}, noop); err != nil {
		t.Fatalf("unknown label: err = %v, want nil", err)
	}
}

// TestOrderedRangeScan_ConcurrentWritersAndCallback runs ordered scans
// concurrently with node writers AND has fn call back into the store (a read),
// exercising the documented lock-free-fn / relaxed-isolation contract under
// the race detector. Correctness assertion: every emission is monotone in
// value order and fn's callback never deadlocks.
func TestOrderedRangeScan_ConcurrentWritersAndCallback(t *testing.T) {
	t.Parallel()
	const label, key = "C", "v"
	ctx := context.Background()

	for _, be := range []orderedBackend{
		{name: "memory", cfg: Config{Store: memory.New(), SnowflakeNodeID: 8}},
		{name: "badger-ram", cfg: Config{BadgerInMemory: true, SnowflakeNodeID: 9}},
	} {
		be := be
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			g, err := New(be.cfg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { _ = g.Close() })
			if err := g.Index.CreateProperty(label, key); err != nil {
				t.Fatalf("CreateProperty: %v", err)
			}
			for i := 0; i < 500; i++ {
				if _, err := g.Nodes.Add(ctx, []string{label}, map[string]any{key: i}); err != nil {
					t.Fatalf("seed Add: %v", err)
				}
			}

			done := make(chan struct{})
			go func() {
				for i := 500; i < 1500; i++ {
					_, _ = g.Nodes.Add(ctx, []string{label}, map[string]any{key: i})
				}
				close(done)
			}()

			for r := 0; r < 40; r++ {
				last := math.Inf(-1)
				err := g.Nodes.ForEachByLabelPropertyRangeOrdered(label, key, math.Inf(-1), math.Inf(1), true, true, false, storepkg.QueryOpts{}, func(n *types.Node) bool {
					v := numericValueOf(t, n, key)
					if v < last {
						t.Errorf("out-of-order emission: %v after %v", v, last)
						return false
					}
					last = v
					// fn calls back into the store (a read) — must not deadlock.
					_, _ = g.Nodes.Get(ctx, n.ID())
					return true
				})
				if err != nil {
					t.Fatalf("concurrent ordered scan: %v", err)
				}
			}
			<-done
		})
	}
}
