package memory_test

import (
	"context"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

type adjOrdEntry struct {
	rel      types.RelID
	relOrd   uint32
	other    types.NodeID
	otherOrd uint32
}

// adjOrdGraph is a memory graph with a small hub (5 out-rels: a slice
// adjacency set), a large hub (100 out-rels, types A and B: a hash set) and a
// sink every one of the 100 nodes points at (type A).
func adjOrdGraph(t *testing.T) (g *graph.Graph, small, large, sink types.NodeID) {
	t.Helper()
	g, err := graph.New(graph.Config{SnowflakeNodeID: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	ctx := context.Background()
	s, _ := g.Nodes().Add(ctx, []string{"P"}, nil)
	l, _ := g.Nodes().Add(ctx, []string{"P"}, nil)
	k, _ := g.Nodes().Add(ctx, []string{"P"}, nil)
	for i := range 100 { // > 64: the large hub's set is a hash set
		n, err := g.Nodes().Add(ctx, []string{"P"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		typ := "A"
		if i%3 == 0 {
			typ = "B"
		}
		if _, err := g.Rels().Add(ctx, typ, l, n, nil); err != nil {
			t.Fatal(err)
		}
		if i < 5 {
			if _, err := g.Rels().Add(ctx, typ, s, n, nil); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := g.Rels().Add(ctx, "A", n, k, nil); err != nil {
			t.Fatal(err)
		}
	}
	return g, s.ID(), l.ID(), k.ID()
}

// The memory door (round 4 R3) emits in relationship-ID order from a pooled
// buffer: over a small (slice) and a large (hash) adjacency set, typed and
// untyped, both directions; a call nested in fn (on another node) returns its
// own sequence and leaves the outer call's entries intact (no shared buffer).
// That a call allocates nothing is TestMemoryAdjacentEndpointOrdinalAllocs, at
// the measurement level (memorystore_adjacent_ordinals_measure_test.go,
// MEMGATE).
func TestMemoryAdjacentEndpointOrdinalOrderNesting(t *testing.T) {
	g, small, large, sink := adjOrdGraph(t)
	ctx := context.Background()
	collect := func(id types.NodeID, typ string, incoming bool) []adjOrdEntry {
		t.Helper()
		var out []adjOrdEntry
		if err := g.Rels().ForEachAdjacentEndpointOrdinal(id, typ, incoming, func(r types.RelID, ro uint32, o types.NodeID, oo uint32) bool {
			out = append(out, adjOrdEntry{r, ro, o, oo})
			return true
		}); err != nil {
			t.Fatal(err)
		}
		return out
	}
	for _, c := range []struct {
		id       types.NodeID
		incoming bool
		typ      string
		want     int
	}{
		{small, false, "", 5}, {small, false, "B", 2},
		{large, false, "", 100}, {large, false, "A", 66}, {large, false, "B", 34},
		{sink, true, "A", 100}, {sink, true, "B", 0}, {sink, false, "", 0},
	} {
		got := collect(c.id, c.typ, c.incoming)
		if len(got) != c.want {
			t.Fatalf("%v incoming=%v type=%q: %d edges, want %d", c.id, c.incoming, c.typ, len(got), c.want)
		}
		if !slices.IsSortedFunc(got, func(a, b adjOrdEntry) int { return int(a.rel.SnowflakeID() - b.rel.SnowflakeID()) }) {
			t.Fatalf("%v incoming=%v type=%q: not in relationship-ID order", c.id, c.incoming, c.typ)
		}
		for _, e := range got {
			if e.relOrd == 0 || e.otherOrd == 0 {
				t.Fatalf("an edge without ordinals: %+v", e)
			}
			// The ordinals equal the entities' own (round 5 request 2 reads
			// the other endpoint's from the relationship's row).
			r, err := g.Rels().Lend(ctx, e.rel)
			if err != nil {
				t.Fatal(err)
			}
			n, err := g.Nodes().Lend(ctx, e.other)
			if err != nil {
				t.Fatal(err)
			}
			if r.Ordinal() != e.relOrd || n.Ordinal() != e.otherOrd {
				t.Fatalf("%+v: ordinals (%d, %d), want the rows' (%d, %d)", e, e.relOrd, e.otherOrd, r.Ordinal(), n.Ordinal())
			}
		}
	}

	// The nested call reads another node, so a buffer shared with the outer
	// call would overwrite the outer call's remaining entries.
	outer := collect(large, "", false)
	inner := collect(sink, "A", true)
	var nestedOuter []adjOrdEntry
	if err := g.Rels().ForEachAdjacentEndpointOrdinal(large, "", false, func(r types.RelID, ro uint32, o types.NodeID, oo uint32) bool {
		nestedOuter = append(nestedOuter, adjOrdEntry{r, ro, o, oo})
		if got := collect(sink, "A", true); !slices.Equal(got, inner) {
			t.Fatal("a nested call saw another sequence")
		}
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(nestedOuter, outer) {
		t.Fatal("the outer call's sequence changed under a nested call")
	}
}
