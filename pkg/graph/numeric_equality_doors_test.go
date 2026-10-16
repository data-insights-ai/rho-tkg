package graph_test

import (
	"bytes"
	"context"
	"errors"
	"math"
	"math/big"
	"slices"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// propertyEqual is the oracle of the property equality doors, written apart
// from the code under test: numbers by exact mathematical value across every
// stored width (a float32 as its float64 widening, -0.0 = 0, +Inf = +Inf),
// strings and booleans by ==, no crossing between kinds, nil and NaN never
// equal.
func propertyEqual(stored, q any) bool {
	if stored == nil || q == nil {
		return false
	}
	a, aNum := exactNumber(stored)
	b, bNum := exactNumber(q)
	if aNum || bNum {
		return aNum && bNum && a != nil && b != nil && a.Cmp(b) == 0
	}
	switch q.(type) {
	case string, bool:
		return stored == q
	}
	return false
}

// exactNumber is v's exact value; nil for NaN, false for a non-number.
func exactNumber(v any) (*big.Float, bool) {
	f := new(big.Float)
	switch x := v.(type) {
	case int:
		return f.SetInt64(int64(x)), true
	case int8:
		return f.SetInt64(int64(x)), true
	case int16:
		return f.SetInt64(int64(x)), true
	case int32:
		return f.SetInt64(int64(x)), true
	case int64:
		return f.SetInt64(x), true
	case uint:
		return f.SetUint64(uint64(x)), true
	case uint8:
		return f.SetUint64(uint64(x)), true
	case uint16:
		return f.SetUint64(uint64(x)), true
	case uint32:
		return f.SetUint64(uint64(x)), true
	case uint64:
		return f.SetUint64(x), true
	case float32:
		if math.IsNaN(float64(x)) {
			return nil, true
		}
		return f.SetFloat64(float64(x)), true
	case float64:
		if math.IsNaN(x) {
			return nil, true
		}
		return f.SetFloat64(x), true
	}
	return nil, false
}

// TestPropertyEqualityDoorsAgree: every property equality door answers one
// rule (propertyEqual) on every backend, with and without the indexes: the
// lookups (ByLabelAndProperty / ByTypeAndProperty, current and at a pin), the
// named valid-time doors, the composite lookup, the paged effective doors'
// filter, and the exact counts (count == len(lookup)), for every numeric width,
// NaN, ±Inf, -0.0, nil and the 2^53 / 2^63 boundaries.
func TestPropertyEqualityDoorsAgree(t *testing.T) {
	nan32 := float32(math.NaN())
	inf32 := float32(math.Inf(1))
	stored := []any{
		int(3), int8(3), int16(3), int32(3), int64(3), uint(3), uint8(3), uint16(3), uint32(3), uint64(3), float32(3), 3.0,
		3.5, float32(3.5), float32(0.1), 0.1, math.NaN(), nan32, math.Inf(1), inf32, math.Inf(-1),
		math.Copysign(0, -1), 0.0, int(0), uint8(0), int64(-3), -3.0, int8(-3),
		float64(1 << 53), int64(1 << 53), int64(1<<53 + 1), uint64(1 << 63), float64(1 << 63), int64(math.MaxInt64),
		uint64(math.MaxUint64), "3", true, int64(4),
	}
	queries := append(slices.Clone(stored), nil, int16(-3), float32(-3), uint32(0), float32(math.Copysign(0, -1)),
		float64(1<<53+1), uint64(1<<53+1), float64(math.MaxUint64), int64(5), "x")

	for _, indexed := range []bool{false, true} {
		mode := "scan"
		if indexed {
			mode = "indexed"
		}
		t.Run(mode, func(t *testing.T) {
			forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
				ctx := context.Background()
				if indexed {
					if err := g.Index().CreateProperty("Ref", "v"); err != nil {
						t.Fatal(err)
					}
					if err := g.Index().CreateRelProperty("R", "v"); err != nil && !b.tiered {
						t.Fatal(err)
					}
					if err := g.Index().CreateComposite("Ref", []string{"v", "w"}); err != nil && !b.tiered {
						t.Fatal(err)
					}
				}
				nodes := make([]*types.Node, len(stored)+1)
				for i := range nodes {
					props := map[string]any{"w": "x"}
					if i < len(stored) {
						props["v"] = stored[i]
					}
					n, err := g.Nodes().Add(ctx, []string{"Ref"}, props)
					if err != nil {
						t.Fatal(err)
					}
					nodes[i] = n
				}
				var pin types.Instant
				for i, v := range stored {
					r, err := g.Rels().Add(ctx, "R", nodes[i], nodes[i+1], map[string]any{"v": v})
					if err != nil {
						t.Fatal(err)
					}
					if from, _, _ := r.TxStamps(); from > pin {
						pin = from
					}
				}

				for _, q := range queries {
					var wantNodes, wantRels []int64
					for i, v := range stored {
						if propertyEqual(v, q) {
							wantNodes = append(wantNodes, int64(nodes[i].ID()))
						}
					}
					if err := g.Rels().ForEachByType("R", storepkg.QueryOpts{}, func(r *types.Relationship) bool {
						if v, _ := r.GetProperty("v"); propertyEqual(v, q) {
							wantRels = append(wantRels, int64(r.ID()))
						}
						return true
					}); err != nil {
						t.Fatal(err)
					}
					slices.Sort(wantRels)
					check := func(door string, got []int64, err error) {
						t.Helper()
						if err != nil {
							t.Errorf("%s(%T %v): %v", door, q, q, err)
							return
						}
						slices.Sort(got)
						got = slices.Compact(got)
						want := wantNodes
						if door[0] == 'R' {
							want = wantRels
						}
						if !slices.Equal(got, want) {
							t.Errorf("%s(%T %v) = %d rows, want %d", door, q, q, len(got), len(want))
						}
					}

					ns, err := g.Nodes().ByLabelAndProperty("Ref", "v", q, storepkg.QueryOpts{})
					check("Nodes.ByLabelAndProperty", nodeIDs(ns), err)
					ns, err = g.Nodes().ByLabelAndProperty("Ref", "v", q, storepkg.QueryOpts{TxPin: pin})
					check("Nodes.ByLabelAndProperty@TxPin", nodeIDs(ns), err)
					ns, err = g.Temporal().NodesByLabelPropertyAt("Ref", "v", q, pin)
					check("Nodes.ByLabelPropertyAt", nodeIDs(ns), err)
					ns, err = g.Nodes().ByLabelAndProperties("Ref", map[string]any{"v": q, "w": "x"}, storepkg.QueryOpts{})
					check("Nodes.ByLabelAndProperties", nodeIDs(ns), err)
					rs, err := g.Rels().ByTypeAndProperty("R", "v", q, storepkg.QueryOpts{})
					check("Rels.ByTypeAndProperty", relIDs(rs), err)
					rs, err = g.Rels().ByTypeAndProperty("R", "v", q, storepkg.QueryOpts{TxPin: pin})
					check("Rels.ByTypeAndProperty@TxPin", relIDs(rs), err)
					rs, err = g.Temporal().RelsByTypePropertyAt("R", "v", q, pin)
					check("Rels.ByTypePropertyAt", relIDs(rs), err)
					if q != nil {
						nsegs, _, err := g.Temporal().NodesEffectivePage(ctx, temporal.Pin{Tx: pin}, temporal.Filter{Prop: "v", Eq: q}, 0, 1000)
						var ids []int64
						for _, s := range nsegs {
							ids = append(ids, int64(s.Node.ID()))
						}
						check("Nodes.EffectivePage", ids, err)
						rsegs, _, err := g.Temporal().RelsEffectivePage(ctx, temporal.Pin{Tx: pin}, temporal.Filter{Prop: "v", Eq: q}, 0, 1000)
						ids = nil
						for _, s := range rsegs {
							ids = append(ids, int64(s.Rel.ID()))
						}
						check("Rels.EffectivePage", ids, err)
					}

					nc, nExact, err := g.Nodes().CountByLabelPropertyValue("Ref", "v", q, storepkg.QueryOpts{})
					if err != nil {
						t.Fatal(err)
					}
					rc, rExact, err := g.Rels().CountByTypePropertyValue("R", "v", q, storepkg.QueryOpts{})
					if err != nil {
						t.Fatal(err)
					}
					if indexed && !b.tiered && (!nExact || !rExact) {
						t.Errorf("Count(%T %v): exact %v / %v, want exact from the index", q, q, nExact, rExact)
					}
					if nExact && nc != int64(len(wantNodes)) {
						t.Errorf("Nodes.CountByLabelPropertyValue(%T %v) = %d, want %d", q, q, nc, len(wantNodes))
					}
					if rExact && rc != int64(len(wantRels)) {
						t.Errorf("Rels.CountByTypePropertyValue(%T %v) = %d, want %d", q, q, rc, len(wantRels))
					}
				}
			})
		})
	}
}

func nodeIDs(ns []*types.Node) []int64 {
	out := make([]int64, len(ns))
	for i, n := range ns {
		out[i] = int64(n.ID())
	}
	return out
}

func relIDs(rs []*types.Relationship) []int64 {
	out := make([]int64, len(rs))
	for i, r := range rs {
		out[i] = int64(r.ID())
	}
	return out
}

// TestPropertyEqualityWriteDoors: compare-and-set, the unique constraints and
// the exact range count follow the same property equality, and a write keeps
// the stored width: CAS with 3.0 over int64(3) succeeds, CAS on a NaN fails,
// int64(3) next to int(3) violates a unique constraint (also UniqueForever),
// RangeCardinalityAt [3, 3] equals the equality count, 2^53+1 is not in
// [2^53, 2^53], and an Update of int64(3) to 3.0 stores the float64.
func TestPropertyEqualityWriteDoors(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		n, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{"v": int64(3), "nan": math.NaN()})
		if err != nil {
			t.Fatal(err)
		}
		if ok, err := g.Nodes().CompareAndSetProperty(ctx, n.ID(), "v", 3.0, int64(4)); err != nil || !ok {
			t.Errorf("CAS(expected 3.0) over int64(3) = %v, %v; want swapped", ok, err)
		}
		if ok, err := g.Nodes().CompareAndSetProperty(ctx, n.ID(), "nan", math.NaN(), 1.0); err != nil || ok {
			t.Errorf("CAS(expected NaN) over NaN = %v, %v; want refused (NaN equals nothing)", ok, err)
		}
		if _, err := g.Nodes().Update(ctx, n.ID(), map[string]any{"v": 4.0}); err != nil {
			t.Fatal(err)
		}
		got, err := g.Nodes().Get(ctx, n.ID())
		if err != nil {
			t.Fatal(err)
		}
		if v, _ := got.GetProperty("v"); v != any(4.0) {
			t.Errorf("Update int64(4) -> 4.0 stored %T %v, want float64 4", v, v)
		}

		for _, forever := range []bool{false, true} {
			if b.tiered {
				break // tiered refuses unique constraints on event-class labels
			}
			label := "U"
			create := g.Constraints().CreateUnique
			if forever {
				label, create = "UF", g.Constraints().CreateUniqueForever
			}
			if err := create(ctx, label, "k"); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Nodes().Add(ctx, []string{label}, map[string]any{"k": int(3)}); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Nodes().Add(ctx, []string{label}, map[string]any{"k": int64(3)}); !errors.Is(err, graphpkg.ErrUniqueViolation) {
				t.Errorf("%s: int64(3) next to int(3): %v, want ErrUniqueViolation", label, err)
			}
			if _, err := g.Nodes().Add(ctx, []string{label}, map[string]any{"k": uint8(4)}); err != nil {
				t.Errorf("%s: uint8(4): %v", label, err)
			}
		}

		if err := g.Index().CreateProperty("Ref", "w"); err != nil {
			t.Fatal(err)
		}
		var pin types.Instant
		for _, v := range []any{int(3), 3.0, float32(3), uint16(3), 3.5, int64(1<<53 + 1), float64(1 << 53)} {
			m, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{"w": v})
			if err != nil {
				t.Fatal(err)
			}
			pin, _, _ = m.TxStamps()
		}
		for _, bound := range []float64{3, 1 << 53} {
			eq, exact, err := g.Nodes().CountByLabelPropertyValue("Ref", "w", bound, storepkg.QueryOpts{})
			if err != nil || (!exact && !b.tiered) {
				t.Fatalf("CountByLabelPropertyValue(%v): exact=%v err=%v", bound, exact, err)
			}
			rc, exact, err := g.Nodes().RangeCardinalityAt("Ref", "w", bound, bound, true, true, storepkg.QueryOpts{TxPin: pin})
			if err != nil {
				t.Fatal(err)
			}
			if exact && !b.tiered && rc != eq {
				t.Errorf("RangeCardinalityAt[%v, %v] = %d, equality count %d", bound, bound, rc, eq)
			}
		}
	})
}

// TestUniqueLegacyWidthTwins (lesson 58: tighten CREATE / CHANGE, keep LOAD
// lenient): int(3) and int64(3) stored under a constraint before v4.57.1 load
// (an import of them is accepted), an update that leaves the constrained value
// alone passes, while a new value equal to them, or a change into one, is
// refused.
func TestUniqueLegacyWidthTwins(t *testing.T) {
	ctx := context.Background()
	src, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	for _, v := range []any{int(3), int64(3), int64(5)} {
		if _, err := src.Nodes().Add(ctx, []string{"User"}, map[string]any{"k": v, "name": "n"}); err != nil {
			t.Fatal(err)
		}
	}
	var buf bytes.Buffer
	if err := src.IO().Export(&buf); err != nil {
		t.Fatal(err)
	}
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if err := g.Constraints().CreateUnique(ctx, "User", "k"); err != nil {
		t.Fatal(err)
	}
	if err := g.IO().Import(bytes.NewReader(buf.Bytes()), tkgio.ImportOptions{}); err != nil {
		t.Fatalf("import of legacy width twins: %v, want accepted (lenient load)", err)
	}
	byValue := map[any]*types.Node{}
	if err := g.Nodes().ForEachByLabel("User", storepkg.QueryOpts{}, func(n *types.Node) bool {
		v, _ := n.GetProperty("k")
		byValue[v] = n
		return true
	}); err != nil {
		t.Fatal(err)
	}
	twin, other := byValue[int64(3)], byValue[int64(5)]
	if twin == nil || other == nil {
		t.Fatalf("imported rows by value = %v", byValue)
	}
	if _, err := g.Nodes().Update(ctx, twin.ID(), map[string]any{"name": "renamed"}); err != nil {
		t.Errorf("update of a twin not touching k: %v, want accepted", err)
	}
	if _, err := g.Nodes().Add(ctx, []string{"User"}, map[string]any{"k": int16(3)}); !errors.Is(err, graphpkg.ErrUniqueViolation) {
		t.Errorf("new int16(3): %v, want ErrUniqueViolation", err)
	}
	if _, err := g.Nodes().Update(ctx, other.ID(), map[string]any{"k": int8(3)}); !errors.Is(err, graphpkg.ErrUniqueViolation) {
		t.Errorf("change 5 -> int8(3): %v, want ErrUniqueViolation", err)
	}
}
