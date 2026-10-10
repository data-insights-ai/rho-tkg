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

// The memory door (round 4 R3) emits in relationship-ID order from a pooled
// buffer: over a small (slice) and a large (hash) adjacency set, typed and
// untyped, both directions; a call nested in fn (on another node) returns its
// own sequence and leaves the outer call's entries intact (no shared buffer);
// a call allocates nothing.
func TestMemoryAdjacentEndpointOrdinalOrderNestingAllocs(t *testing.T) {
	g, err := graph.New(graph.Config{SnowflakeNodeID: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	ctx := context.Background()
	small, _ := g.Nodes().Add(ctx, []string{"P"}, nil)
	large, _ := g.Nodes().Add(ctx, []string{"P"}, nil)
	sink, _ := g.Nodes().Add(ctx, []string{"P"}, nil)
	for i := range 100 { // > 64: the large hub's set is a hash set
		n, err := g.Nodes().Add(ctx, []string{"P"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		typ := "A"
		if i%3 == 0 {
			typ = "B"
		}
		if _, err := g.Rels().Add(ctx, typ, large, n, nil); err != nil {
			t.Fatal(err)
		}
		if i < 5 {
			if _, err := g.Rels().Add(ctx, typ, small, n, nil); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := g.Rels().Add(ctx, "A", n, sink, nil); err != nil {
			t.Fatal(err)
		}
	}
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
		{small.ID(), false, "", 5}, {small.ID(), false, "B", 2},
		{large.ID(), false, "", 100}, {large.ID(), false, "A", 66}, {large.ID(), false, "B", 34},
		{sink.ID(), true, "A", 100}, {sink.ID(), true, "B", 0}, {sink.ID(), false, "", 0},
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
		}
	}

	// The nested call reads another node, so a buffer shared with the outer
	// call would overwrite the outer call's remaining entries.
	outer := collect(large.ID(), "", false)
	inner := collect(sink.ID(), "A", true)
	var nestedOuter []adjOrdEntry
	if err := g.Rels().ForEachAdjacentEndpointOrdinal(large.ID(), "", false, func(r types.RelID, ro uint32, o types.NodeID, oo uint32) bool {
		nestedOuter = append(nestedOuter, adjOrdEntry{r, ro, o, oo})
		if got := collect(sink.ID(), "A", true); !slices.Equal(got, inner) {
			t.Fatal("a nested call saw another sequence")
		}
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(nestedOuter, outer) {
		t.Fatal("the outer call's sequence changed under a nested call")
	}

	if raceEnabled {
		return // sync.Pool drops items at random under -race
	}
	var seen int
	fn := func(types.RelID, uint32, types.NodeID, uint32) bool { seen++; return true }
	id := large.ID()
	if a := testing.AllocsPerRun(100, func() {
		if err := g.Rels().ForEachAdjacentEndpointOrdinal(id, "A", false, fn); err != nil {
			t.Fatal(err)
		}
	}); a != 0 {
		t.Fatalf("%v allocations per call, want 0", a)
	}
}
