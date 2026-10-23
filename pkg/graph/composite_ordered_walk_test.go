package graph_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"strings"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/ingest"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The stated gaps of the composite stores (sigma-tkgd round 10, twodriver
// D5): ordered index walks, stated bounds, relationship class counts and
// exact range counts on sharded (several slots, written through ingest
// lanes) and tiered (labels and types spanning the reference and an event
// shard), against brute-force oracles over the current rows, on every
// backend; a second phase after updates, deletes and value moves.

// r10CompositeBackends are the four backends plus a sharded store whose
// entities land on several slots (ingest lanes).
func r10CompositeBackends() []storeBackend {
	out := allStoreBackends()
	out = append(out, storeBackend{name: "sharded-lanes", open: func(t *testing.T) *graphpkg.Graph {
		st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 6})
		if err != nil {
			t.Fatal(err)
		}
		g, err := graphpkg.New(graphpkg.Config{Store: st, SnowflakeNodeID: 0, IngestLanes: 4})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = g.Close() })
		return g
	}})
	return out
}

// r10CompositeValues are the walked values: equal int and float of one
// value, widths, a float32, -0.0, values past 2^53, NaN, strings.
var r10CompositeValues = []any{
	int64(3), 3.0, float32(3), int8(3), uint64(3), int64(-5), -5.0, 0.0, math.Copysign(0, -1), float32(0.1), 0.1,
	int64(1<<53 + 1), float64(1 << 53), uint64(1<<63 + 7), math.Inf(1), math.Inf(-1), math.NaN(), "s", int64(7), 12.5,
}

var r10Strings = []string{"", "a", "ab", "abc", "abd", "b", "ba", "zz", "a\xff", "ab"}

// r10BoundValues are the bounds key's values (no integer past 2^53: an index
// holding one states no bounds).
var r10BoundValues = []any{int64(-5), -5.0, float32(-5), int8(9), 9.0, uint64(9), 1.5, int64(0), "x", true, []string{"l"}, math.NaN()}

// r10CompositeWrite writes n nodes (labels A, half also Ref: on tiered the
// reference shard; through one ingest session each, so a laned sharded store
// spreads them over its slots) and a relationship of type T from each to
// the next.
func r10CompositeWrite(t *testing.T, g *graphpkg.Graph, rng *rand.Rand, n int) ([]*types.Node, []*types.Relationship) {
	t.Helper()
	props := func() map[string]any {
		p := map[string]any{}
		if rng.IntN(8) != 0 {
			p["v"] = r10CompositeValues[rng.IntN(len(r10CompositeValues))]
		}
		if rng.IntN(6) != 0 {
			p["s"] = r10Strings[rng.IntN(len(r10Strings))]
		}
		if rng.IntN(5) != 0 {
			p["b"] = r10BoundValues[rng.IntN(len(r10BoundValues))]
		}
		return p
	}
	var nodes []*types.Node
	var rels []*types.Relationship
	for i := range n {
		labels := []string{"A"}
		if i%2 == 0 {
			labels = []string{"Ref", "A"}
		}
		sess, err := g.Ingest().NewSession(ingest.IngestOptions{Concurrent: true})
		if err != nil {
			t.Fatal(err)
		}
		nd, err := sess.AddNode(labels, props())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := sess.Submit(); err != nil {
			t.Fatal(err)
		}
		_ = sess.Close()
		nodes = append(nodes, nd)
	}
	ctx := context.Background()
	for i := range n {
		r, err := g.Rels().Add(ctx, "T", nodes[i], nodes[(i+1)%n], props())
		if err != nil {
			t.Fatal(err)
		}
		rels = append(rels, r)
	}
	return nodes, rels
}

// r10Row is one walked row as the oracle sees it.
type r10Row struct {
	id    int64
	value any
	has   bool
}

// r10CheckWalk checks an ordered numeric walk: every row whose value lies in
// [min, max] (exactly, flags applied) is walked once, every walked row has
// a number whose index sort key lies in the ulp-widened range, rows are in
// sort-key order (descending when desc), ties by ID ascending.
func r10CheckWalk(t *testing.T, what string, all, walked []r10Row, min, max float64, inclMin, inclMax, desc bool) {
	t.Helper()
	seen := map[int64]bool{}
	lo, hi := math.Nextafter(min, math.Inf(-1)), math.Nextafter(max, math.Inf(1))
	for i, r := range walked {
		if seen[r.id] {
			t.Fatalf("%s: row %d walked twice", what, r.id)
		}
		seen[r.id] = true
		k, ok := numericKey(r.value)
		if !r.has || !ok || k < lo || k > hi {
			t.Fatalf("%s: row %d holds %T %v, outside the range", what, r.id, r.value, r.value)
		}
		if i > 0 {
			pk, _ := numericKey(walked[i-1].value)
			if (desc && pk < k) || (!desc && pk > k) || (pk == k && walked[i-1].id > r.id) {
				t.Fatalf("%s: row %d (%v) after row %d (%v): out of order (desc %v)", what, r.id, k, walked[i-1].id, pk, desc)
			}
		}
	}
	for _, r := range all {
		if !r.has {
			continue
		}
		cl, okL := types.CompareNumber(r.value, min)
		ch, okH := types.CompareNumber(r.value, max)
		in := okL && okH && (cl > 0 || (cl == 0 && inclMin)) && (ch < 0 || (ch == 0 && inclMax))
		if in && !seen[r.id] {
			t.Fatalf("%s: row %d holds %T %v in [%v, %v] but was not walked", what, r.id, r.value, r.value, min, max)
		}
	}
}

// r10CheckPrefixWalk checks an ordered prefix walk: exactly the rows whose
// string value starts with prefix, by (value, ID), descending values when desc.
func r10CheckPrefixWalk(t *testing.T, what string, all, walked []r10Row, prefix string, desc bool) {
	t.Helper()
	want := 0
	for _, r := range all {
		if s, ok := r.value.(string); r.has && ok && strings.HasPrefix(s, prefix) {
			want++
		}
	}
	if len(walked) != want {
		t.Fatalf("%s: %d rows walked, %d hold a string with prefix %q", what, len(walked), want, prefix)
	}
	for i, r := range walked {
		s, ok := r.value.(string)
		if !ok || !strings.HasPrefix(s, prefix) {
			t.Fatalf("%s: row %d holds %v", what, r.id, r.value)
		}
		if i > 0 {
			p := walked[i-1].value.(string)
			if (desc && p < s) || (!desc && p > s) || (p == s && walked[i-1].id > r.id) {
				t.Fatalf("%s: %q (row %d) after %q (row %d)", what, s, r.id, p, walked[i-1].id)
			}
		}
	}
}

// TestRound10CompositeOrderedWalksAndStats: on every backend, a laned
// sharded store and tiered spanning shards included, before and after a
// second phase of updates and deletes:
//   - ForEachByLabelPropertyRangeOrdered / ForEachByTypePropertyRangeOrdered
//     walk the rows in range in the index's order (random bounds and
//     flags, both directions), a stop after k rows gives the first k;
//   - ForEachByLabelPropertyPrefix / ForEachByTypePropertyPrefix likewise;
//   - RangeCardinality / RelRangeCardinality are exact and equal the exact
//     count of the rows in range (int and float of one value, past 2^53);
//   - PropertyBounds / RelPropertyBounds equal the brute-force bounds, the
//     stored value at each of the preferred width, the class partition;
//   - RelPropertyTypeClassCounts equal the brute-force partition.
func TestRound10CompositeOrderedWalksAndStats(t *testing.T) {
	t.Parallel()
	n, rounds := 120, 12
	if testing.Short() {
		n, rounds = 60, 6
	}
	for _, b := range r10CompositeBackends() {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			for _, k := range []string{"v", "s", "b"} {
				if err := g.Index().CreateProperty("A", k); err != nil {
					t.Fatal(err)
				}
				if err := g.Index().CreateRelProperty("T", k); err != nil {
					t.Fatal(err)
				}
			}
			rng := rand.New(rand.NewPCG(10, 10))
			nodes, rels := r10CompositeWrite(t, g, rng, n)
			if b.name == "sharded-lanes" {
				slots := map[int64]bool{}
				for _, nd := range nodes {
					slots[g.Admin().DecomposeNodeID(nd.ID()).NodeID] = true
				}
				if len(slots) < 2 {
					t.Fatalf("nodes on %d slot(s), want several", len(slots))
				}
			}
			exactCounts := 0
			check := func(phase string) {
				t.Helper()
				nodeRows := func(key string) []r10Row {
					var out []r10Row
					if err := g.Nodes().ForEachByLabel("A", storepkg.QueryOpts{}, func(nd *types.Node) bool {
						v, ok := nd.GetProperty(key)
						out = append(out, r10Row{int64(nd.ID()), v, ok})
						return true
					}); err != nil {
						t.Fatal(err)
					}
					return out
				}
				relRows := func(key string) []r10Row {
					var out []r10Row
					if err := g.Rels().ForEachByType("T", storepkg.QueryOpts{}, func(r *types.Relationship) bool {
						v, ok := r.GetProperty(key)
						out = append(out, r10Row{int64(r.ID()), v, ok})
						return true
					}); err != nil {
						t.Fatal(err)
					}
					return out
				}
				edges := []float64{math.Inf(-1), -5, -1, 0, 0.1, 3, 7, 1 << 53, 1<<53 + 2, math.Inf(1)}
				for range rounds {
					min, max := edges[rng.IntN(len(edges))], edges[rng.IntN(len(edges))]
					if min > max {
						min, max = max, min
					}
					inclMin, inclMax, desc := rng.IntN(2) == 0, rng.IntN(2) == 0, rng.IntN(2) == 0
					what := fmt.Sprintf("%s %s [%v, %v] incl %v/%v desc %v", b.name, phase, min, max, inclMin, inclMax, desc)
					var walkedN, walkedR []r10Row
					if err := g.Nodes().ForEachByLabelPropertyRangeOrdered("A", "v", min, max, inclMin, inclMax, desc, storepkg.QueryOpts{}, func(nd *types.Node) bool {
						v, ok := nd.GetProperty("v")
						walkedN = append(walkedN, r10Row{int64(nd.ID()), v, ok})
						return true
					}); err != nil {
						t.Fatalf("%s nodes: %v", what, err)
					}
					r10CheckWalk(t, what+" nodes", nodeRows("v"), walkedN, min, max, inclMin, inclMax, desc)
					if err := g.Rels().ForEachByTypePropertyRangeOrdered("T", "v", min, max, inclMin, inclMax, desc, storepkg.QueryOpts{}, func(r *types.Relationship) bool {
						v, ok := r.GetProperty("v")
						walkedR = append(walkedR, r10Row{int64(r.ID()), v, ok})
						return true
					}); err != nil {
						t.Fatalf("%s rels: %v", what, err)
					}
					r10CheckWalk(t, what+" rels", relRows("v"), walkedR, min, max, inclMin, inclMax, desc)
					if k := len(walkedR) / 2; k > 0 {
						var first []r10Row
						if err := g.Rels().ForEachByTypePropertyRangeOrdered("T", "v", min, max, inclMin, inclMax, desc, storepkg.QueryOpts{}, func(r *types.Relationship) bool {
							v, ok := r.GetProperty("v")
							first = append(first, r10Row{int64(r.ID()), v, ok})
							return len(first) < k
						}); err != nil {
							t.Fatal(err)
						}
						if fmt.Sprint(first) != fmt.Sprint(walkedR[:k]) {
							t.Fatalf("%s: a walk stopped after %d rows differs from the full walk's first %d", what, k, k)
						}
					}
					// The range count's rule is the index's: a value by its sort
					// key (numericKey: a float32 by its shortest decimal), not
					// the equality's float32 widening (see the round 10 audit).
					exact := func(rows []r10Row) int64 {
						var c int64
						for _, r := range rows {
							k, num := numericKey(r.value)
							if r.has && num && (k > min || (k == min && inclMin)) && (k < max || (k == max && inclMax)) {
								c++
							}
						}
						return c
					}
					// The index counts by float64 sort keys: where a row holds an
					// integer past 2^53 the count may decline (exact=false), as
					// on a single store; the "b" key holds none and is always
					// exact. A served count is exact.
					imprecise := func(rows []r10Row) bool {
						for _, r := range rows {
							if c, ok := types.CompareNumber(r.value, 1<<53); ok && c > 0 {
								return true
							}
							if c, ok := types.CompareNumber(r.value, -(1 << 53)); ok && c < 0 {
								return true
							}
						}
						return false
					}
					for _, key := range []string{"v", "b"} {
						nr, rr := nodeRows(key), relRows(key)
						c, ok, err := g.Nodes().RangeCardinality("A", key, min, max, inclMin, inclMax, storepkg.QueryOpts{})
						if err != nil || (ok && !imprecise(nr) && c != exact(nr)) || (!ok && !imprecise(nr)) {
							t.Fatalf("%s node range count of %s %d exact %v err %v, want %d", what, key, c, ok, err, exact(nr))
						}
						exactCounts += map[bool]int{true: 1}[ok]
						c, ok, err = g.Rels().RangeCardinality("T", key, min, max, inclMin, inclMax, storepkg.QueryOpts{})
						if err != nil || (ok && !imprecise(rr) && c != exact(rr)) || (!ok && !imprecise(rr)) {
							t.Fatalf("%s rel range count of %s %d exact %v err %v, want %d", what, key, c, ok, err, exact(rr))
						}
						exactCounts += map[bool]int{true: 1}[ok]
					}
				}
				for _, prefix := range []string{"", "a", "ab", "b", "zz", "q"} {
					for _, desc := range []bool{false, true} {
						var wn, wr []r10Row
						if err := g.Nodes().ForEachByLabelPropertyPrefix("A", "s", prefix, desc, storepkg.QueryOpts{}, func(nd *types.Node) bool {
							v, ok := nd.GetProperty("s")
							wn = append(wn, r10Row{int64(nd.ID()), v, ok})
							return true
						}); err != nil {
							t.Fatalf("%s prefix %q: %v", b.name, prefix, err)
						}
						r10CheckPrefixWalk(t, fmt.Sprintf("%s %s nodes prefix %q desc %v", b.name, phase, prefix, desc), nodeRows("s"), wn, prefix, desc)
						if err := g.Rels().ForEachByTypePropertyPrefix("T", "s", prefix, desc, storepkg.QueryOpts{}, func(r *types.Relationship) bool {
							v, ok := r.GetProperty("s")
							wr = append(wr, r10Row{int64(r.ID()), v, ok})
							return true
						}); err != nil {
							t.Fatalf("%s rel prefix %q: %v", b.name, prefix, err)
						}
						r10CheckPrefixWalk(t, fmt.Sprintf("%s %s rels prefix %q desc %v", b.name, phase, prefix, desc), relRows("s"), wr, prefix, desc)
					}
				}
				nb, ok, err := g.Stats().PropertyBounds("A", "b", storepkg.QueryOpts{})
				if err != nil || !ok {
					t.Fatalf("%s %s node bounds: ok %v err %v", b.name, phase, ok, err)
				}
				nodeBoundsOracle(t, g, "A", "b").check(t, b.name+" "+phase+" node bounds", nb)
				rb, ok, err := g.Stats().RelPropertyBounds("T", "b", storepkg.QueryOpts{})
				if err != nil || !ok {
					t.Fatalf("%s %s rel bounds: ok %v err %v", b.name, phase, ok, err)
				}
				relBoundsOracle(t, g, "T", "b").check(t, b.name+" "+phase+" rel bounds", rb)
				for _, key := range []string{"v", "b"} {
					got, err := g.Stats().RelPropertyTypeClassCounts("T", key)
					if err != nil {
						t.Fatalf("%s %s rel class counts: %v", b.name, phase, err)
					}
					var vals []any
					var present []bool
					for _, r := range relRows(key) {
						vals, present = append(vals, r.value), append(present, r.has)
					}
					if want := oracleBounds(vals, present).want.Classes; got != want {
						t.Fatalf("%s %s rel class counts of %s %+v, want %+v", b.name, phase, key, got, want)
					}
				}
			}
			check("created")
			// Phase 2: values moved, rows deleted (both endpoints' shards).
			for i, nd := range nodes {
				switch i % 5 {
				case 0:
					if _, err := g.Nodes().Update(ctx, nd.ID(), map[string]any{"v": float64(i) + 0.5, "s": "ab" + fmt.Sprint(i), "b": int64(-9)}); err != nil {
						t.Fatal(err)
					}
				case 1:
					if _, err := g.Rels().Update(ctx, rels[i].ID(), map[string]any{"v": int64(-i), "b": 100.0}); err != nil {
						t.Fatal(err)
					}
				case 2:
					if err := g.Rels().Delete(ctx, rels[i].ID()); err != nil {
						t.Fatal(err)
					}
				}
			}
			check("moved")
			if exactCounts < rounds {
				t.Fatalf("%s: only %d exact range counts", b.name, exactCounts)
			}
		})
	}
}

// TestRound10CompositeWalkRefusesWithoutIndex: a walk over a pair without an
// index, or with an Elements index, is ErrIndexNotFound on every backend
// (a part without a usable index fails the composite walk); the bounds
// decline.
func TestRound10CompositeWalkRefusesWithoutIndex(t *testing.T) {
	t.Parallel()
	for _, b := range r10CompositeBackends() {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			r10CompositeWrite(t, g, rand.New(rand.NewPCG(1, 1)), 8)
			if err := g.Index().CreatePropertyWithOptions("A", "e", storepkg.PropertyIndexOptions{Elements: true}); err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{"v", "e"} {
				err := g.Nodes().ForEachByLabelPropertyRangeOrdered("A", key, 0, 1, true, true, false, storepkg.QueryOpts{}, func(*types.Node) bool { return true })
				if !errors.Is(err, storepkg.ErrIndexNotFound) {
					t.Fatalf("%s key %s: %v, want ErrIndexNotFound", b.name, key, err)
				}
				if _, ok, err := g.Stats().PropertyBounds("A", key, storepkg.QueryOpts{}); err != nil || ok {
					t.Fatalf("%s key %s bounds: ok %v err %v, want a decline", b.name, key, ok, err)
				}
			}
			err := g.Rels().ForEachByTypePropertyPrefix("T", "s", "", false, storepkg.QueryOpts{}, func(*types.Relationship) bool { return true })
			if !errors.Is(err, storepkg.ErrIndexNotFound) {
				t.Fatalf("%s rel prefix without an index: %v", b.name, err)
			}
		})
	}
}
