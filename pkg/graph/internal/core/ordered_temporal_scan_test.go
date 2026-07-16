package core

import (
	"context"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// namesOf projects the emitted nodes to their "name" property, in emission order.
func namesOf(t *testing.T, nodes []*types.Node) []string {
	t.Helper()
	out := make([]string, 0, len(nodes))
	for _, n := range nodes {
		v, ok := n.GetProperty("name")
		if !ok {
			t.Fatalf("node %d missing name", n.ID().SnowflakeID())
		}
		out = append(out, v.(string))
	}
	return out
}

func eqStr(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestTemporalOrderedScan_Numeric is the rule-15/16 proof for Stage B: a NUMERIC
// ordered range scan pinned to a past valid-time returns nodes ordered by their
// value AS OF that time — including a node whose value was IN range then but is
// OUT of range now (the current-state index provably cannot answer this). Runs on
// memory, badger-RAM, and badger-disk; the temporal fold needs no property index.
func TestTemporalOrderedScan_Numeric(t *testing.T) {
	t.Parallel()
	const label, key = "Metric", "score"
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

			// Two-phase lifecycles (valid_from tiles v0 [1000,2000), v1 [2000,∞)):
			//   A: 10 -> 100  (in range at t0, OUT at t_now)
			//   B: 20 -> 5    (in range at both, value REORDERS across time)
			//   C: 30         (never updated)
			mk := func(name string, s0 float64) *types.Node {
				n, err := g.Nodes.Add(ctx, []string{label}, map[string]any{"name": name, "tkg_valid_from": types.Instant(1000), key: s0})
				if err != nil {
					t.Fatalf("Add(%s): %v", name, err)
				}
				return n
			}
			upd := func(n *types.Node, s1 float64) {
				if _, err := g.Nodes.Update(ctx, n.ID(), map[string]any{"tkg_valid_from": types.Instant(2000), key: s1}); err != nil {
					t.Fatalf("Update: %v", err)
				}
			}
			a := mk("A", 10)
			b := mk("B", 20)
			mk("C", 30)
			upd(a, 100)
			upd(b, 5)

			collect := func(min, max float64, desc bool, validAt types.Instant) []string {
				var got []*types.Node
				err := g.Nodes.ForEachByLabelPropertyRangeOrdered(label, key, min, max, true, true, desc,
					storepkg.QueryOpts{ValidAt: validAt}, func(n *types.Node) bool {
						got = append(got, n)
						return true
					})
				if err != nil {
					t.Fatalf("ordered range temporal: %v", err)
				}
				return namesOf(t, got)
			}

			// AS OF valid t=1500 (v0 for A,B): A=10,B=20,C=30 all in [0,50].
			if got := collect(0, 50, false, 1500); !eqStr(got, []string{"A", "B", "C"}) {
				t.Errorf("asc [0,50] @1500 = %v, want [A B C]", got)
			}
			// AS OF valid t=2500 (v1 for A,B): A=100 (OUT), B=5, C=30.
			// asc by value-at-t: B(5), C(30). A excluded — the case the index misses.
			if got := collect(0, 50, false, 2500); !eqStr(got, []string{"B", "C"}) {
				t.Errorf("asc [0,50] @2500 = %v, want [B C] (A=100 out of range)", got)
			}
			// desc @2500: C(30), B(5).
			if got := collect(0, 50, true, 2500); !eqStr(got, []string{"C", "B"}) {
				t.Errorf("desc [0,50] @2500 = %v, want [C B]", got)
			}
			// Wide range @2500 asc: B(5), C(30), A(100) — REORDER vs t0 (A was first).
			if got := collect(0, 1000, false, 2500); !eqStr(got, []string{"B", "C", "A"}) {
				t.Errorf("asc [0,1000] @2500 = %v, want [B C A]", got)
			}
			// Same wide range @1500 asc: A(10), B(20), C(30) — the earlier ordering.
			if got := collect(0, 1000, false, 1500); !eqStr(got, []string{"A", "B", "C"}) {
				t.Errorf("asc [0,1000] @1500 = %v, want [A B C]", got)
			}
			_ = b
		})
	}
}

// TestTemporalOrderedScan_Prefix is the string mirror: a prefix scan pinned to a
// past valid-time orders by value-at-t, including a node whose value matched the
// prefix then but not now.
func TestTemporalOrderedScan_Prefix(t *testing.T) {
	t.Parallel()
	const label, key = "Word", "text"
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

			mk := func(name, s0 string) *types.Node {
				n, err := g.Nodes.Add(ctx, []string{label}, map[string]any{"name": name, "tkg_valid_from": types.Instant(1000), key: s0})
				if err != nil {
					t.Fatalf("Add(%s): %v", name, err)
				}
				return n
			}
			upd := func(n *types.Node, s1 string) {
				if _, err := g.Nodes.Update(ctx, n.ID(), map[string]any{"tkg_valid_from": types.Instant(2000), key: s1}); err != nil {
					t.Fatalf("Update: %v", err)
				}
			}
			// A: "apple" -> "banana"  (matches "ap" then, NOT now)
			// B: "april" -> "apex"    (matches "ap" at both; reorders: april>apex)
			a := mk("A", "apple")
			b := mk("B", "april")
			upd(a, "banana")
			upd(b, "apex")

			collect := func(prefix string, desc bool, validAt types.Instant) []string {
				var got []*types.Node
				err := g.Nodes.ForEachByLabelPropertyPrefix(label, key, prefix, desc,
					storepkg.QueryOpts{ValidAt: validAt}, func(n *types.Node) bool {
						got = append(got, n)
						return true
					})
				if err != nil {
					t.Fatalf("prefix temporal: %v", err)
				}
				return namesOf(t, got)
			}

			// @1500: apple(A), april(B) both match "ap"; asc lex: apple < april -> A,B.
			if got := collect("ap", false, 1500); !eqStr(got, []string{"A", "B"}) {
				t.Errorf("asc 'ap' @1500 = %v, want [A B]", got)
			}
			// @2500: A="banana" (NO match), B="apex" (match). Only B.
			if got := collect("ap", false, 2500); !eqStr(got, []string{"B"}) {
				t.Errorf("asc 'ap' @2500 = %v, want [B] (A=banana no longer matches)", got)
			}
			_ = b
		})
	}
}

// TestTemporalOrderedScan_RelPrefix is the relationship mirror (memory + badger).
func TestTemporalOrderedScan_RelPrefix(t *testing.T) {
	t.Parallel()
	const typeName, key = "LINK", "tag"
	ctx := context.Background()

	for _, be := range relPrefixBackends() {
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
			r1, err := g.Rels.Add(ctx, typeName, a, b, map[string]any{"name": "R1", "tkg_valid_from": types.Instant(1000), key: "apple"})
			if err != nil {
				t.Fatalf("Add rel: %v", err)
			}
			r2, err := g.Rels.Add(ctx, typeName, a, b, map[string]any{"name": "R2", "tkg_valid_from": types.Instant(1000), key: "april"})
			if err != nil {
				t.Fatalf("Add rel: %v", err)
			}
			if _, err := g.Rels.Update(ctx, r1.ID(), map[string]any{"tkg_valid_from": types.Instant(2000), key: "banana"}); err != nil {
				t.Fatalf("Update rel: %v", err)
			}
			_ = r2

			collect := func(prefix string, validAt types.Instant) []string {
				var out []string
				err := g.Rels.ForEachByTypePropertyPrefix(typeName, key, prefix, false,
					storepkg.QueryOpts{ValidAt: validAt}, func(r *types.Relationship) bool {
						v, _ := r.GetProperty("name")
						out = append(out, v.(string))
						return true
					})
				if err != nil {
					t.Fatalf("rel prefix temporal: %v", err)
				}
				return out
			}

			// @1500: R1(apple), R2(april) both match; asc lex: apple<april -> R1,R2.
			if got := collect("ap", 1500); !eqStr(got, []string{"R1", "R2"}) {
				t.Errorf("asc 'ap' @1500 = %v, want [R1 R2]", got)
			}
			// @2500: R1="banana" (no match), R2="april". Only R2.
			if got := collect("ap", 2500); !eqStr(got, []string{"R2"}) {
				t.Errorf("asc 'ap' @2500 = %v, want [R2]", got)
			}
		})
	}
}
