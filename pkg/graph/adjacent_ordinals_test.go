package graph_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

type adjacentOrdinal struct {
	rel      types.RelID
	relOrd   uint32
	other    types.NodeID
	otherOrd uint32
}

func adjacentOrdinals(t *testing.T, g *graphpkg.Graph, id types.NodeID, typeName string, incoming bool) []adjacentOrdinal {
	t.Helper()
	var out []adjacentOrdinal
	if err := g.Rels().ForEachAdjacentEndpointOrdinal(id, typeName, incoming, func(rel types.RelID, relOrd uint32, other types.NodeID, otherOrd uint32) bool {
		out = append(out, adjacentOrdinal{rel, relOrd, other, otherOrd})
		return true
	}); err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(out, func(a, b adjacentOrdinal) int { return int(a.rel.SnowflakeID() - b.rel.SnowflakeID()) })
	return out
}

// The door yields exactly ForEachAdjacentEndpoint's (relationship, other
// endpoint) pairs, in both directions, with and without a type, each with
// the ordinals Lend reports for the relationship and the other endpoint (0
// where the store numbers none). Two-phase: after a relationship is deleted
// it is gone and the rest keep their ordinals. An unknown type yields
// nothing, a missing node is ErrNodeNotFound, fn returning false stops.
func TestForEachAdjacentEndpointOrdinal(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		hub, err := g.Nodes().Add(ctx, []string{"H"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		var spokes []*types.Node
		for i := range 6 {
			n, err := g.Nodes().Add(ctx, []string{"S"}, map[string]any{"i": int64(i)})
			if err != nil {
				t.Fatal(err)
			}
			spokes = append(spokes, n)
			typ := "A"
			if i%2 == 1 {
				typ = "B"
			}
			if _, err := g.Rels().Add(ctx, typ, hub, n, nil); err != nil {
				t.Fatal(err)
			}
			if i < 3 {
				if _, err := g.Rels().Add(ctx, "A", n, hub, nil); err != nil {
					t.Fatal(err)
				}
			}
		}
		check := func(step string) []adjacentOrdinal {
			t.Helper()
			var first []adjacentOrdinal
			for _, incoming := range []bool{false, true} {
				for _, typ := range []string{"", "A", "B"} {
					got := adjacentOrdinals(t, g, hub.ID(), typ, incoming)
					var want []adjacentOrdinal
					if err := g.Rels().ForEachAdjacentEndpoint(hub.ID(), typ, incoming, func(rel types.RelID, other types.NodeID) bool {
						r, err := g.Rels().Lend(ctx, rel)
						if err != nil {
							t.Fatal(err)
						}
						n, err := g.Nodes().Lend(ctx, other)
						if err != nil {
							t.Fatal(err)
						}
						want = append(want, adjacentOrdinal{rel, r.Ordinal(), other, n.Ordinal()})
						return true
					}); err != nil {
						t.Fatal(err)
					}
					slices.SortFunc(want, func(a, b adjacentOrdinal) int { return int(a.rel.SnowflakeID() - b.rel.SnowflakeID()) })
					if !slices.Equal(got, want) {
						t.Fatalf("%s incoming=%v type=%q:\n got %v\nwant %v", step, incoming, typ, got, want)
					}
					numbered := b.name == "memory" || b.name == "badger" || b.name == "sharded"
					for _, e := range got {
						if numbered && (e.relOrd == 0 || e.otherOrd == 0) {
							t.Fatalf("%s: an edge without ordinals on %s: %+v", step, b.name, e)
						}
					}
					if !incoming && typ == "" {
						first = got
					}
				}
			}
			return first
		}
		before := check("first")
		if len(before) != 6 {
			t.Fatalf("%d outgoing edges, want 6", len(before))
		}
		if err := g.Rels().Delete(ctx, before[0].rel); err != nil {
			t.Fatal(err)
		}
		after := check("after a delete")
		if len(after) != 5 || after[0] != before[1] {
			t.Fatalf("after the delete: %v", after)
		}

		if got := adjacentOrdinals(t, g, hub.ID(), "NOPE", false); len(got) != 0 {
			t.Fatalf("an unknown type yielded %d", len(got))
		}
		stops := 0
		if err := g.Rels().ForEachAdjacentEndpointOrdinal(hub.ID(), "", false, func(types.RelID, uint32, types.NodeID, uint32) bool {
			stops++
			return false
		}); err != nil || stops != 1 {
			t.Fatalf("early stop: %d calls, %v", stops, err)
		}
		if err := g.Nodes().Delete(ctx, spokes[5].ID()); err != nil { // cascades its relationship
			t.Fatal(err)
		}
		for _, e := range check("after a node delete") {
			if e.other == spokes[5].ID() {
				t.Fatal("a deleted endpoint is still adjacent")
			}
		}
		missing := types.NodeID(1)
		if err := g.Rels().ForEachAdjacentEndpointOrdinal(missing, "", false, func(types.RelID, uint32, types.NodeID, uint32) bool { return true }); !errors.Is(err, graphpkg.ErrNodeNotFound) {
			t.Fatalf("a missing node: %v", err)
		}
		if err := g.Rels().ForEachAdjacentEndpointOrdinal(hub.ID(), "", false, nil); err == nil {
			t.Fatal("a nil callback passed")
		}
	})
}

// A declared segment type's relationships carry ordinal 0; their endpoints
// keep theirs.
func TestForEachAdjacentEndpointOrdinalSegmentType(t *testing.T) {
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 4, Store: memory.New(), RelSegments: []graphpkg.RelSegmentSpec{{
		Type: "HOP", Columns: []graphpkg.SegmentColumn{{Name: "w", Kind: graphpkg.SegmentInt64}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	ctx := context.Background()
	a, _ := g.Nodes().Add(ctx, []string{"P"}, nil)
	b, _ := g.Nodes().Add(ctx, []string{"P"}, nil)
	if _, err := g.Rels().Add(ctx, "HOP", a, b, map[string]any{"w": int64(1)}); err != nil {
		t.Fatal(err)
	}
	if err := g.Admin().SealRelSegments("HOP"); err != nil {
		t.Fatal(err)
	}
	end, err := g.Nodes().Lend(ctx, b.ID())
	if err != nil {
		t.Fatal(err)
	}
	got := adjacentOrdinals(t, g, a.ID(), "HOP", false)
	if len(got) != 1 || got[0].relOrd != 0 || got[0].other != b.ID() || got[0].otherOrd != end.Ordinal() || end.Ordinal() == 0 {
		t.Fatalf("segment edge: %+v (end ordinal %d)", got, end.Ordinal())
	}
}
