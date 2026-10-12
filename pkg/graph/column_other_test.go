package graph_test

import (
	"context"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 48: ColumnData.Other marks a cell whose row holds the property outside
// the column (a list, a map, another kind, a width the columns do not carry).

// otherProps puts the column with Other cells SECOND, so a mark on the wrong
// column index lands on w, which has none. s is held only as an int8, so its
// column never resolves a kind.
var otherProps = []string{"w", "v", "s"}

// otherCell is one cell as a column scan reports it.
type otherCell struct {
	null, other bool
	i           int64
}

// wantOtherCell classifies a row read the way the columns must: absent, an
// int64 (the kind of every column here), or Other with a null, zero slot.
func wantOtherCell(raw any, found bool) otherCell {
	if !found {
		return otherCell{null: true}
	}
	if x, ok := raw.(int64); ok {
		return otherCell{i: x}
	}
	return otherCell{null: true, other: true}
}

// gotOtherCells reads every row of a batch, failing on a column that is not
// parallel to IDs (a misaligned typed slot or a short Other column).
func gotOtherCells(t *testing.T, cd *storepkg.ColumnData, n int) [][]otherCell {
	t.Helper()
	if cd.Other != nil && len(cd.Other) != len(otherProps) {
		t.Fatalf("len(Other)=%d, want %d", len(cd.Other), len(otherProps))
	}
	rows := make([][]otherCell, n)
	for k := range rows {
		rows[k] = make([]otherCell, len(otherProps))
	}
	for c := range otherProps {
		if cd.Kinds[c] != storepkg.ColInt64 || len(cd.Null[c]) != n || len(cd.Ints[c]) != n ||
			(cd.Other != nil && len(cd.Other[c]) != n) {
			t.Fatalf("column %d: kind=%d len(Null)=%d len(Ints)=%d, want int64 columns of %d rows (Other %v)",
				c, cd.Kinds[c], len(cd.Null[c]), len(cd.Ints[c]), n, cd.Other)
		}
		for k := range n {
			rows[k][c] = otherCell{null: cd.Null[c][k], other: cd.Other != nil && cd.Other[c][k], i: cd.Ints[c][k]}
		}
	}
	return rows
}

// TestColumnScanOther_MatchesRowRead compares every cell of a node and a
// relationship column scan with the row read under the same opts, on every
// backend and on declared segment relationships: current rows, a ValidAt and a
// TxAt pin taken before one row's list was fixed and another row broken to a map.
func TestColumnScanOther_MatchesRowRead(t *testing.T) {
	segments := storeBackend{name: "memory-segments", open: func(t *testing.T) *graphpkg.Graph {
		g, err := graphpkg.New(graphpkg.Config{Validation: graphpkg.ValidationLimits{AllowSelfLoops: true}, SnowflakeNodeID: 4, Store: memory.New(), RelSegments: []graphpkg.RelSegmentSpec{{
			Type: "R", Columns: []graphpkg.SegmentColumn{
				{Name: "w", Kind: graphpkg.SegmentInt64}, {Name: "v", Kind: graphpkg.SegmentInt64}, {Name: "s", Kind: graphpkg.SegmentInt8}},
		}}})
		if err != nil {
			t.Fatalf("graph.New: %v", err)
		}
		t.Cleanup(func() { _ = g.Close() })
		return g
	}}
	for _, b := range append(allStoreBackends(), segments) {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			pin := buildOtherFixture(t, g, b.name == segments.name)
			for _, opts := range []graphpkg.QueryOpts{{}, {ValidAt: 1500}, {TxAt: pin}} {
				want := map[types.EntityID][]otherCell{}
				nodes, err := g.Nodes().ByLabel("P", opts)
				if err != nil {
					t.Fatalf("ByLabel(%+v): %v", opts, err)
				}
				for _, n := range nodes {
					for _, p := range otherProps {
						raw, found := n.GetProperty(p)
						want[types.EntityID(n.ID())] = append(want[types.EntityID(n.ID())], wantOtherCell(raw, found))
					}
				}
				got := map[types.EntityID][]otherCell{}
				ok, err := g.ScanNodeColumns("P", otherProps, opts, func(cb *graphpkg.ColumnBatch) bool {
					for k, row := range gotOtherCells(t, &cb.ColumnData, len(cb.IDs)) {
						got[types.EntityID(cb.IDs[k])] = row
					}
					return true
				})
				assertOtherCells(t, b, "nodes", opts, ok, err, got, want)

				want = map[types.EntityID][]otherCell{}
				rels, err := g.Rels().ByType("R", opts)
				if err != nil {
					t.Fatalf("ByType(%+v): %v", opts, err)
				}
				for _, r := range rels {
					for _, p := range otherProps {
						raw, found := r.GetProperty(p)
						want[types.EntityID(r.ID())] = append(want[types.EntityID(r.ID())], wantOtherCell(raw, found))
					}
				}
				got = map[types.EntityID][]otherCell{}
				ok, err = g.ScanRelColumns("R", otherProps, opts, func(rb *graphpkg.RelColumnBatch) bool {
					for k, row := range gotOtherCells(t, &rb.ColumnData, len(rb.IDs)) {
						got[types.EntityID(rb.IDs[k])] = row
					}
					return true
				})
				assertOtherCells(t, b, "rels", opts, ok, err, got, want)
			}
		})
	}
}

// buildOtherFixture writes five nodes (label P) and five relationships (type R)
// with the same values, returns a TxAt pin after them, then fixes row 0's list
// to an int and breaks row 1's int to a map, both from valid time 2000.
func buildOtherFixture(t *testing.T, g *graphpkg.Graph, seal bool) types.Instant {
	t.Helper()
	ctx := context.Background()
	rows := []map[string]any{
		{"w": int64(1), "v": []any{int64(1), int64(2)}}, // list, first in ID order
		{"w": int64(2), "v": int64(7)},
		{"w": int64(3), "v": map[string]any{"k": int64(1)}}, // map
		{"w": int64(4), "v": "seven"},                       // kind mismatch
		{"w": int64(5), "s": int8(3)},                       // v missing; s a width no column carries
	}
	a, err := g.Nodes().Add(ctx, []string{"H"}, nil)
	if err != nil {
		t.Fatalf("add endpoint: %v", err)
	}
	var nodes []types.NodeID
	var rels []types.RelID
	var pin types.Instant
	for _, props := range rows {
		props["tkg_valid_from"] = types.Instant(1000)
		n, err := g.Nodes().Add(ctx, []string{"P"}, props)
		if err != nil {
			t.Fatalf("add node %v: %v", props, err)
		}
		r, err := g.Rels().Add(ctx, "R", a, a, props)
		if err != nil {
			t.Fatalf("add rel %v: %v", props, err)
		}
		nodes, rels, pin = append(nodes, n.ID()), append(rels, r.ID()), r.Temporal().TxFrom
	}
	if seal {
		if err := g.Admin().SealRelSegments("R"); err != nil {
			t.Fatalf("SealRelSegments: %v", err)
		}
	}
	for i, upd := range []map[string]any{
		{"v": int64(9), "tkg_valid_from": types.Instant(2000)},
		{"v": map[string]any{"k": int64(2)}, "tkg_valid_from": types.Instant(2000)},
	} {
		if _, err := g.Nodes().Update(ctx, nodes[i], upd); err != nil {
			t.Fatalf("update node %d: %v", i, err)
		}
		if _, err := g.Rels().Update(ctx, rels[i], upd); err != nil {
			t.Fatalf("update rel %d: %v", i, err)
		}
	}
	return pin
}

func assertOtherCells(t *testing.T, b storeBackend, what string, opts graphpkg.QueryOpts, ok bool, err error,
	got, want map[types.EntityID][]otherCell) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s %+v: scan: %v", what, opts, err)
	}
	if !ok {
		if columnScanNative(b) || b.name == "memory-segments" {
			t.Fatalf("%s %+v: ok=false on %s, which has the capability", what, opts, b.name)
		}
		return // tiered, sharded: no column scan, nothing filled
	}
	if len(want) != 5 {
		t.Fatalf("%s %+v: reference holds %d rows, want 5", what, opts, len(want))
	}
	others := 0
	for id, w := range want {
		g, seen := got[id]
		if !seen {
			t.Errorf("%s %+v: row %d missing from the scan", what, opts, id)
			continue
		}
		for c := range otherProps {
			if g[c] != w[c] {
				t.Errorf("%s %+v: row %d column %s = %+v, row read says %+v", what, opts, id, otherProps[c], g[c], w[c])
			}
			if w[c].other {
				others++
			}
		}
	}
	if len(got) != len(want) {
		t.Errorf("%s %+v: scan holds %d rows, want %d", what, opts, len(got), len(want))
	}
	if others != 4 {
		t.Fatalf("%s %+v: fixture holds %d Other cells, want 4", what, opts, others)
	}
}
