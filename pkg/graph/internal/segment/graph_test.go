package segment_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/internal/synthhop"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/integrity"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/segment"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// hopSchema is HOP after ai-soc P6: four string columns; valid and
// transaction time are system columns.
func hopSchema(token uint16) segment.Schema {
	return segment.Schema{TypeName: "HOP", TypeToken: token, Columns: []segment.Column{
		{Name: "actor", Kind: segment.KindString}, {Name: "asset_class", Kind: segment.KindString},
		{Name: "family", Kind: segment.KindString}, {Name: "orch", Kind: segment.KindString},
	}}
}

// writeHOP writes a synthday-shaped HOP workload through the real create
// doors of an in-memory graph and returns the stored rows (ByType).
func writeHOP(tb testing.TB, sz synthhop.Size, schema synthhop.Schema) (*graph.Graph, []*types.Relationship) {
	tb.Helper()
	ctx := context.Background()
	g, err := graph.New(graph.Config{Validation: graph.ValidationLimits{AllowSelfLoops: true}, AllowTxBackfill: true})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = g.Close() })
	var nodes []*types.Node
	err = synthhop.Generate(synthhop.Config{Size: sz, Schema: schema, Workload: synthhop.WorkloadHOP},
		func(n synthhop.Node) error {
			node, err := g.Nodes().Add(ctx, []string{"Asset"}, n.Props)
			nodes = append(nodes, node)
			return err
		},
		func(e synthhop.Edge) error {
			_, err := g.Rels().AddWithTx(ctx, e.Type, nodes[e.Start], nodes[e.End], e.Props, types.Instant(e.TxFrom))
			return err
		})
	if err != nil {
		tb.Fatal(err)
	}
	rows, err := g.Rels().ByType("HOP", graph.QueryOpts{})
	if err != nil {
		tb.Fatal(err)
	}
	return g, rows
}

// TestRealKernelRows_HashesAndFieldsSurvive encodes rows exactly as the
// create kernel stored them (real IDs, hashes, endpoint hashes, backfilled
// transaction time) and requires every decoded row to recompute its stored
// hash and to equal the stored row field by field.
func TestRealKernelRows_HashesAndFieldsSurvive(t *testing.T) {
	sz := synthhop.Size{Name: "t", HOP: 3000, Pairs: 900, Hosts: 120, Actors: 30}
	for _, schema := range []synthhop.Schema{synthhop.SchemaP6, synthhop.SchemaLegacy} {
		_, rows := writeHOP(t, sz, schema)
		data, err := segment.Encode(hopSchema(rows[0].TypeToken().Value()), rows, segment.Options{})
		if err != nil {
			t.Fatal(err)
		}
		seg, err := segment.Open(data)
		if err != nil {
			t.Fatal(err)
		}
		stored := map[types.RelID]*types.Relationship{}
		for _, r := range rows {
			stored[r.ID()] = r
		}
		n := 0
		if err := seg.Scan(func(_ int, got *types.Relationship) bool {
			n++
			want := stored[got.ID()]
			if h := integrity.ComputeRelHash(got, "HOP"); h != want.Integrity().Hash {
				t.Fatalf("row %d: recomputed %s, stored %s", got.ID(), h, want.Integrity().Hash)
			}
			if *got.Temporal() != *want.Temporal() || !reflect.DeepEqual(got.Integrity(), want.Integrity()) ||
				got.StartNodeID() != want.StartNodeID() || got.EndNodeID() != want.EndNodeID() {
				t.Fatalf("row %d differs: %+v %+v vs %+v %+v", got.ID(), got.Temporal(), got.Integrity(), want.Temporal(), want.Integrity())
			}
			return true
		}); err != nil {
			t.Fatal(err)
		}
		if n != len(rows) {
			t.Fatalf("scanned %d of %d", n, len(rows))
		}
		if err := seg.Verify(); err != nil {
			t.Fatal(err)
		}
	}
}
