package graph_test

import (
	"context"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// RC4 at the public door, across BOTH backends — the level where a consumer meets
// it, and the only level that proves the capability is wired end to end (facade ->
// core -> store) rather than merely implemented on a store.

// TestScanRelColumns_EndToEndBothBackends asserts the facade returns the same
// (start, end, weight) a row read would, on each backend.
func TestScanRelColumns_EndToEndBothBackends(t *testing.T) {
	eachBackend(t, func(t *testing.T, g *graphpkg.Graph) {
		ctx := context.Background()
		a, err := g.Nodes().Add(ctx, []string{"City"}, map[string]any{"name": "a"})
		if err != nil {
			t.Fatalf("AddNode a: %v", err)
		}
		b, err := g.Nodes().Add(ctx, []string{"City"}, map[string]any{"name": "b"})
		if err != nil {
			t.Fatalf("AddNode b: %v", err)
		}
		rel, err := g.Rels().Add(ctx, "ROAD", a, b,
			map[string]any{"weight": int64(42)})
		if err != nil {
			t.Fatalf("AddRelationship: %v", err)
		}

		type row struct {
			id           types.RelID
			start, end   types.NodeID
			weight       int64
			weightAbsent bool
		}
		var got []row
		ok, err := g.ScanRelColumns("ROAD", []string{"weight"}, graphpkg.QueryOpts{},
			func(batch *graphpkg.RelColumnBatch) bool {
				ii := 0
				for i := range batch.IDs {
					r := row{id: batch.IDs[i], start: batch.StartIDs[i], end: batch.EndIDs[i]}
					if batch.Null[0][i] {
						r.weightAbsent = true
						ii++
					} else {
						r.weight = batch.Ints[0][ii]
						ii++
					}
					got = append(got, r)
				}
				return true
			})
		if err != nil {
			t.Fatalf("ScanRelColumns: %v", err)
		}
		if !ok {
			t.Fatal("backend reported no relationship column scan capability; both " +
				"backends implement it, so ok=false means the wiring is missing")
		}
		if len(got) != 1 {
			t.Fatalf("got %d rows, want 1", len(got))
		}
		r := got[0]
		if r.id != rel.InternalID() {
			t.Errorf("id: got %v, want %v", r.id, rel.InternalID())
		}
		if r.start != a.InternalID() || r.end != b.InternalID() {
			t.Errorf("endpoints: got (%v->%v), want (%v->%v)",
				r.start, r.end, a.InternalID(), b.InternalID())
		}
		if r.weightAbsent || r.weight != 42 {
			t.Errorf("weight: got %d (absent=%v), want 42", r.weight, r.weightAbsent)
		}
	})
}

// TestScanRelColumns_UnknownTypeIsZeroRowsNotUnsupported pins the two meanings of
// ok. A type nobody has used is a KNOWN capability with no rows (ok=true), not an
// unsupported backend (ok=false) — a caller that conflated them would take the row
// fallback for every empty type and never notice.
func TestScanRelColumns_UnknownTypeIsZeroRowsNotUnsupported(t *testing.T) {
	eachBackend(t, func(t *testing.T, g *graphpkg.Graph) {
		rows := 0
		ok, err := g.ScanRelColumns("NO_SUCH_TYPE", []string{"weight"}, graphpkg.QueryOpts{},
			func(batch *graphpkg.RelColumnBatch) bool { rows += len(batch.IDs); return true })
		if err != nil {
			t.Fatalf("ScanRelColumns: %v", err)
		}
		if !ok {
			t.Fatal("unknown relationship type reported ok=false (capability missing); " +
				"it must report ok=true with zero rows")
		}
		if rows != 0 {
			t.Fatalf("unknown type emitted %d rows", rows)
		}
	})
}

// TestScanRelColumns_NilGraphDoesNotPanic — the facade's nil guard, matching
// ScanNodeColumns.
func TestScanRelColumns_NilGraphDoesNotPanic(t *testing.T) {
	var g *graphpkg.Graph
	ok, err := g.ScanRelColumns("ROAD", nil, graphpkg.QueryOpts{},
		func(batch *graphpkg.RelColumnBatch) bool { return true })
	if ok || err != nil {
		t.Fatalf("nil graph: got ok=%v err=%v, want false/nil", ok, err)
	}
}
