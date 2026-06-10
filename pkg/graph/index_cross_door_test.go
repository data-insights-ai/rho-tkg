package graph_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Index cross-door tests: an index is a second door to the same answer. A
// property/temporal index that returns even one row more or fewer than the
// brute-force scan is silently wrong forever. Twin-graph design: graph I
// (indexed) and graph U (never indexed) receive IDENTICAL operation streams;
// every query must agree exactly at every checkpoint — including after
// mutations that the 3-phase index build (lesson 11) must not lose, and for
// special float values whose printable form loses equality detail (lesson 23).

type twinGraphs struct {
	indexed, plain *graphpkg.Graph
}

func newTwins(t *testing.T) twinGraphs {
	t.Helper()
	gi, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 2})
	if err != nil {
		t.Fatalf("indexed twin: %v", err)
	}
	t.Cleanup(func() { gi.Close() })
	gu, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 3})
	if err != nil {
		t.Fatalf("plain twin: %v", err)
	}
	t.Cleanup(func() { gu.Close() })
	return twinGraphs{indexed: gi, plain: gu}
}

// both applies the same mutation to both twins and returns the node from each.
func (tw twinGraphs) both(t *testing.T, op func(g *graphpkg.Graph) (*types.Node, error)) (*types.Node, *types.Node) {
	t.Helper()
	ni, err := op(tw.indexed)
	if err != nil {
		t.Fatalf("indexed twin op: %v", err)
	}
	nu, err := op(tw.plain)
	if err != nil {
		t.Fatalf("plain twin op: %v", err)
	}
	return ni, nu
}

// assertQueryAgreement compares a ByLabelAndProperty answer across twins by
// the "who" property (IDs differ across graphs).
func assertQueryAgreement(t *testing.T, tw twinGraphs, label, key string, value any, opts storepkg.QueryOpts, what string) {
	t.Helper()
	gotI, err := tw.indexed.Nodes().ByLabelAndProperty(label, key, value, opts)
	if err != nil {
		t.Fatalf("%s: indexed query: %v", what, err)
	}
	gotU, err := tw.plain.Nodes().ByLabelAndProperty(label, key, value, opts)
	if err != nil {
		t.Fatalf("%s: plain query: %v", what, err)
	}
	if fmt.Sprint(whoSet(gotI)) != fmt.Sprint(whoSet(gotU)) {
		t.Errorf("%s: indexed door %v != brute-force door %v (value=%v)", what, whoSet(gotI), whoSet(gotU), value)
	}
}

func whoSet(nodes []*types.Node) []string {
	out := make([]string, 0, len(nodes))
	for _, n := range nodes {
		w, _ := n.GetProperty("who")
		out = append(out, fmt.Sprint(w))
	}
	// nodes are ID-sorted per graph but IDs differ across twins; sort by who.
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

func TestPropertyIndexAgreesWithBruteForceTwin(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tw := newTwins(t)

	add := func(who string, score any) {
		tw.both(t, func(g *graphpkg.Graph) (*types.Node, error) {
			return g.Nodes().Add(ctx, []string{"Scored"}, map[string]any{"who": who, "score": score})
		})
	}
	// Adversarial value matrix: duplicate values, special floats (lesson 23:
	// index keys must preserve the equality contract, including NaN and ±0),
	// and type-distinct numerics.
	add("a", int64(10))
	add("b", int64(10))
	add("c", float64(10)) // float 10 vs int 10 — must NOT collide
	add("d", math.NaN())
	add("e", math.Copysign(0, -1)) // -0.0
	add("f", float64(0))           // +0.0
	add("g", "10")                 // string "10" vs numeric 10

	// Index the property on the indexed twin only.
	if err := tw.indexed.Index().CreateProperty("Scored", "score"); err != nil {
		t.Fatalf("CreateProperty: %v", err)
	}

	queries := []any{int64(10), float64(10), math.NaN(), math.Copysign(0, -1), float64(0), "10", int64(999)}
	for _, q := range queries {
		assertQueryAgreement(t, tw, "Scored", "score", q, storepkg.QueryOpts{}, "post-build")
	}

	// Mutate THROUGH the index's lifetime: change values, delete, add — the
	// index must track every mutation door.
	tw.both(t, func(g *graphpkg.Graph) (*types.Node, error) {
		rows, err := g.Nodes().ByLabelAndProperty("Scored", "who", "a", storepkg.QueryOpts{})
		if err != nil || len(rows) != 1 {
			return nil, fmt.Errorf("locate a: %v (%d)", err, len(rows))
		}
		if err := g.Nodes().SetProperty(ctx, rows[0].ID(), "score", int64(20)); err != nil {
			return nil, err
		}
		return rows[0], nil
	})
	tw.both(t, func(g *graphpkg.Graph) (*types.Node, error) {
		rows, err := g.Nodes().ByLabelAndProperty("Scored", "who", "b", storepkg.QueryOpts{})
		if err != nil || len(rows) != 1 {
			return nil, fmt.Errorf("locate b: %v (%d)", err, len(rows))
		}
		return nil, g.Nodes().Delete(ctx, rows[0].ID())
	})
	add("h", int64(10))

	for _, q := range queries {
		assertQueryAgreement(t, tw, "Scored", "score", q, storepkg.QueryOpts{}, "post-mutation")
	}
	assertQueryAgreement(t, tw, "Scored", "score", int64(20), storepkg.QueryOpts{}, "post-mutation-new-value")
}

// Lesson 11's exact risk: the 3-phase index build runs concurrently with
// writers. Whatever interleaving happens, the final index must agree with
// the never-indexed twin that received the same final state.
func TestPropertyIndexBuiltUnderConcurrentWritesAgrees(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tw := newTwins(t)

	const n = 60
	ids := make([]types.NodeID, n)
	for i := 0; i < n; i++ {
		who := fmt.Sprintf("n%02d", i)
		ni, _ := tw.both(t, func(g *graphpkg.Graph) (*types.Node, error) {
			return g.Nodes().Add(ctx, []string{"Live"}, map[string]any{"who": who, "v": int64(i % 5)})
		})
		ids[i] = ni.ID()
	}

	// Writers churn the indexed twin while the index builds; the SAME final
	// values are applied to the plain twin afterwards (deterministic final
	// state, racy build interleaving).
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			_ = tw.indexed.Nodes().SetProperty(ctx, ids[i], "v", int64(i%3))
		}
	}()
	if err := tw.indexed.Index().CreateProperty("Live", "v"); err != nil {
		t.Fatalf("CreateProperty under writes: %v", err)
	}
	wg.Wait()

	// Bring the plain twin to the identical final state.
	plainRows, err := tw.plain.Nodes().ByLabel("Live", storepkg.QueryOpts{})
	if err != nil || len(plainRows) != n {
		t.Fatalf("plain twin scan: %v (%d)", err, len(plainRows))
	}
	for _, row := range plainRows {
		w, _ := row.GetProperty("who")
		var i int
		fmt.Sscanf(fmt.Sprint(w), "n%02d", &i)
		if err := tw.plain.Nodes().SetProperty(ctx, row.ID(), "v", int64(i%3)); err != nil {
			t.Fatalf("plain twin set: %v", err)
		}
	}

	for v := int64(0); v < 5; v++ {
		assertQueryAgreement(t, tw, "Live", "v", v, storepkg.QueryOpts{}, "post-concurrent-build")
	}
}

// Temporal-index cross-door: after CreateTemporal, ValidAt/interval queries
// must agree exactly with the never-indexed twin on an adversarial timeline
// (explicit closed/open intervals, snowflake fallback, label churn, deletion).
func TestTemporalIndexAgreesWithBruteForceTwin(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tw := newTwins(t)

	add := func(who string, props map[string]any) {
		p := map[string]any{"who": who}
		for k, v := range props {
			p[k] = v
		}
		tw.both(t, func(g *graphpkg.Graph) (*types.Node, error) {
			return g.Nodes().Add(ctx, []string{"Ev"}, p)
		})
	}
	add("a", map[string]any{"tkg_valid_from": types.Instant(1000), "tkg_valid_to": types.Instant(2000)})
	add("b", map[string]any{"tkg_valid_from": types.Instant(1500)})
	add("c", nil) // snowflake fallback
	add("d", map[string]any{"tkg_valid_from": types.Instant(1000)})
	// d: label removed later; e: deleted.
	tw.both(t, func(g *graphpkg.Graph) (*types.Node, error) {
		rows, err := g.Nodes().ByLabelAndProperty("Ev", "who", "d", storepkg.QueryOpts{})
		if err != nil || len(rows) != 1 {
			return nil, fmt.Errorf("locate d: %v", err)
		}
		if err := g.Nodes().AddLabel(ctx, rows[0].ID(), "Keep"); err != nil {
			return nil, err
		}
		return nil, g.Nodes().RemoveLabel(ctx, rows[0].ID(), "Ev")
	})
	add("e", map[string]any{"tkg_valid_from": types.Instant(1000)})
	tw.both(t, func(g *graphpkg.Graph) (*types.Node, error) {
		rows, err := g.Nodes().ByLabelAndProperty("Ev", "who", "e", storepkg.QueryOpts{})
		if err != nil || len(rows) != 1 {
			return nil, fmt.Errorf("locate e: %v", err)
		}
		return nil, g.Nodes().Delete(ctx, rows[0].ID())
	})

	if err := tw.indexed.Index().CreateTemporal("Ev"); err != nil {
		t.Fatalf("CreateTemporal: %v", err)
	}

	now := types.Instant(time.Now().UnixMilli())
	compare := func(opts storepkg.QueryOpts, what string) {
		t.Helper()
		gi, err := tw.indexed.Nodes().ByLabel("Ev", opts)
		if err != nil {
			t.Fatalf("%s indexed: %v", what, err)
		}
		gu, err := tw.plain.Nodes().ByLabel("Ev", opts)
		if err != nil {
			t.Fatalf("%s plain: %v", what, err)
		}
		if fmt.Sprint(whoSet(gi)) != fmt.Sprint(whoSet(gu)) {
			t.Errorf("%s: indexed %v != brute force %v", what, whoSet(gi), whoSet(gu))
		}
	}
	compare(storepkg.QueryOpts{ValidAt: 1200}, "ValidAt=1200")
	compare(storepkg.QueryOpts{ValidAt: 1700}, "ValidAt=1700")
	compare(storepkg.QueryOpts{ValidAt: now + 3_600_000}, "ValidAt=future")
	compare(storepkg.QueryOpts{ValidStart: 1100, ValidEnd: 1600}, "During[1100,1600)")
	compare(storepkg.QueryOpts{ValidStart: 2000, ValidEnd: 2100}, "During-meets")
}

// Pagination cross-door: walking pages must reproduce the unpaged set
// EXACTLY (no duplicates, no omissions), including with temporal opts and a
// property index active; a hostile cursor must be classified, not obeyed.
func TestPaginationUnionEqualsUnpagedSet(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 4})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	for i := 0; i < 23; i++ {
		props := map[string]any{"who": fmt.Sprintf("p%02d", i)}
		if i%3 == 0 {
			props["tkg_valid_from"] = types.Instant(1000)
			props["tkg_valid_to"] = types.Instant(2000)
		}
		if _, err := g.Nodes().Add(ctx, []string{"Page"}, props); err != nil {
			t.Fatalf("add: %v", err)
		}
	}
	if err := g.Index().CreateProperty("Page", "who"); err != nil {
		t.Fatalf("index: %v", err)
	}

	for name, opts := range map[string]storepkg.QueryOpts{
		"plain":    {},
		"temporal": {ValidAt: 1500},
	} {
		unpaged, err := g.Nodes().ByLabel("Page", opts)
		if err != nil {
			t.Fatalf("%s unpaged: %v", name, err)
		}
		var union []*types.Node
		pageOpts := opts
		pageOpts.Limit = 4
		seen := map[types.NodeID]bool{}
		for {
			page, err := g.Nodes().ByLabel("Page", pageOpts)
			if err != nil {
				t.Fatalf("%s page: %v", name, err)
			}
			if len(page) == 0 {
				break
			}
			for _, n := range page {
				if seen[n.ID()] {
					t.Fatalf("%s: duplicate %v across pages", name, n.ID())
				}
				seen[n.ID()] = true
				union = append(union, n)
			}
			pageOpts.After = types.EntityID(page[len(page)-1].ID())
			if len(page) < pageOpts.Limit {
				break
			}
		}
		if fmt.Sprint(whoSet(union)) != fmt.Sprint(whoSet(unpaged)) {
			t.Errorf("%s: paged union %v != unpaged %v", name, whoSet(union), whoSet(unpaged))
		}
	}

	// Hostile cursor: a negative cursor must be rejected with the documented
	// sentinel, never treated as a valid position.
	_, err = g.Nodes().ByLabel("Page", storepkg.QueryOpts{Limit: 4, After: types.EntityID(-1)})
	if !errors.Is(err, graphpkg.ErrInvalidQueryCursor) {
		t.Errorf("hostile cursor error = %v, want ErrInvalidQueryCursor", err)
	}
}
