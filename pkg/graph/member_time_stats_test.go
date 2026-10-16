package graph_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// nodeMemberTimeOracle folds every row of the tracked entities: members and
// explicit valid time from the current rows carrying the label, LastEnded
// over the history rows of each entity some row of which carried it.
func nodeMemberTimeOracle(t *testing.T, g *graphpkg.Graph, label string, ids []types.NodeID) storepkg.MemberTimeStats {
	t.Helper()
	ctx := context.Background()
	var out storepkg.MemberTimeStats
	for _, id := range ids {
		var rows []*types.Node
		cur, err := g.Nodes().Get(ctx, id)
		switch {
		case err == nil:
			if slices.Contains(g.Nodes().Labels(cur), label) {
				out.Members++
				if from, to, _ := cur.ValidRange(); from != 0 || to != 0 {
					out.ExplicitValid++
				}
			}
		case !errors.Is(err, graphpkg.ErrNodeNotFound):
			t.Fatal(err)
		}
		hist, err := g.Nodes().History(id)
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, hist...)
		member := cur != nil && slices.Contains(g.Nodes().Labels(cur), label)
		var last types.Instant
		for _, r := range rows {
			if slices.Contains(g.Nodes().Labels(r), label) {
				member = true
			}
			_, to, del := r.TxStamps()
			last = max(last, to, del)
		}
		if member {
			out.LastEnded = max(out.LastEnded, last)
		}
	}
	return out
}

func relMemberTimeOracle(t *testing.T, g *graphpkg.Graph, typeName string, ids []types.RelID) storepkg.MemberTimeStats {
	t.Helper()
	ctx := context.Background()
	var out storepkg.MemberTimeStats
	for _, id := range ids {
		var rows []*types.Relationship
		cur, err := g.Rels().Get(ctx, id)
		switch {
		case err == nil:
			out.Members++
			if from, to, _ := cur.ValidRange(); from != 0 || to != 0 {
				out.ExplicitValid++
			}
		case !errors.Is(err, graphpkg.ErrRelNotFound):
			t.Fatal(err)
		}
		hist, err := g.Rels().History(id)
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, hist...)
		for _, r := range rows {
			_, to, del := r.TxStamps()
			out.LastEnded = max(out.LastEnded, to, del)
		}
	}
	return out
}

// TestMemberTimeStats (sigma-tkgd round 8 request 3): Stats().LabelTimeStats
// / RelTypeTimeStats equal a fold over every row of every entity through
// untimed creates (eternal), an explicit valid-from, a closing valid-to, an
// untimed update (a row ended), deletes, label moves and writes to other
// labels and types; served from the cache between writes (exact on memory
// and badger), never stale after a write.
func TestMemberTimeStats(t *testing.T) {
	forAllStoreBackendsTxFrom(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		label := "Ref"
		var ids []types.NodeID
		var relIDs []types.RelID
		native := b.name == "memory" || b.name == "badger"
		check := func(stage string) {
			t.Helper()
			for range 2 { // the second call is served from the cache
				got, exact, err := g.Stats().LabelTimeStats(label)
				if err != nil {
					t.Fatalf("%s: %v", stage, err)
				}
				if want := nodeMemberTimeOracle(t, g, label, ids); got != want {
					t.Fatalf("%s: LabelTimeStats = %+v, want %+v", stage, got, want)
				}
				rgot, rexact, err := g.Stats().RelTypeTimeStats("R")
				if err != nil {
					t.Fatalf("%s rel: %v", stage, err)
				}
				if want := relMemberTimeOracle(t, g, "R", relIDs); rgot != want {
					t.Fatalf("%s: RelTypeTimeStats = %+v, want %+v", stage, rgot, want)
				}
				if native && (!exact || !rexact) {
					t.Fatalf("%s: exact %v / %v without writers", stage, exact, rexact)
				}
			}
		}
		check("empty")
		hub, err := g.Nodes().Add(ctx, []string{"Hub"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		for range 5 {
			n, err := g.Nodes().Add(ctx, []string{label}, map[string]any{"v": int64(1)})
			if err != nil {
				t.Fatal(err)
			}
			ids = append(ids, n.ID())
			r, err := g.Rels().Add(ctx, "R", hub, n, nil)
			if err != nil {
				t.Fatal(err)
			}
			relIDs = append(relIDs, r.ID())
		}
		check("untimed")
		if st, _, _ := g.Stats().LabelTimeStats(label); !st.Eternal() || st.Members != 5 {
			t.Fatalf("untimed rows not eternal: %+v", st)
		}
		timed, err := g.Nodes().Add(ctx, []string{label}, map[string]any{"tkg_valid_from": types.Instant(1000)})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, timed.ID())
		r, err := g.Rels().Add(ctx, "R", hub, timed, map[string]any{"tkg_valid_from": types.Instant(1000)})
		if err != nil {
			t.Fatal(err)
		}
		relIDs = append(relIDs, r.ID())
		check("explicit valid-from")
		pinBefore, _, _ := timed.TxStamps()
		up, err := g.Nodes().Update(ctx, ids[0], map[string]any{"v": int64(2)})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := g.Rels().Update(ctx, relIDs[0], map[string]any{"w": int64(2)}); err != nil {
			t.Fatal(err)
		}
		check("untimed update")
		st, _, _ := g.Stats().LabelTimeStats(label)
		ended, _, _ := up.TxStamps()
		if !st.HistoryAfter(pinBefore) || st.HistoryAfter(ended) || st.Eternal() {
			t.Fatalf("HistoryAfter: %+v, pin before %d, update at %d", st, pinBefore, ended)
		}
		if _, err := g.Nodes().Update(ctx, ids[1], map[string]any{"tkg_valid_to": types.Instant(1 << 50)}); err != nil {
			t.Fatal(err)
		}
		check("closing valid-to")
		until, err := g.Nodes().Add(ctx, []string{label}, map[string]any{"tkg_valid_to": types.Instant(1 << 50)})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, until.ID())
		if from, to, _ := until.ValidRange(); from != 0 || to == 0 {
			t.Fatalf("valid-to only row: from %d to %d", from, to)
		}
		check("valid-to only")
		if err := g.Nodes().Delete(ctx, ids[2]); err != nil {
			t.Fatal(err)
		}
		check("delete")
		other, err := g.Nodes().Add(ctx, []string{"Other"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := g.Nodes().Update(ctx, other.ID(), map[string]any{"x": int64(1)}); err != nil {
			t.Fatal(err)
		}
		s, err := g.Rels().Add(ctx, "S", hub, other, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := g.Rels().Update(ctx, s.ID(), map[string]any{"x": int64(1)}); err != nil {
			t.Fatal(err)
		}
		check("other label and type written")
		if !b.tiered { // tiered refuses label changes across its shard classes
			if err := g.Nodes().AddLabel(ctx, ids[3], "Keep"); err != nil {
				t.Fatal(err)
			}
			if err := g.Nodes().RemoveLabel(ctx, ids[3], label); err != nil {
				t.Fatal(err)
			}
			check("label removed")
			// The moved entity stays an ever-member: its next row end counts.
			if _, err := g.Nodes().Update(ctx, ids[3], map[string]any{"v": int64(9)}); err != nil {
				t.Fatal(err)
			}
			check("former member updated")
		}

		if _, ok, err := g.Stats().LabelTimeStats("Nope"); err != nil || !ok {
			t.Fatalf("unknown label: ok=%v err=%v", ok, err)
		}
		if _, _, err := g.Stats().LabelTimeStats(""); err == nil {
			t.Fatal("empty label: no error")
		}
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := g.Stats().LabelTimeStats(label); !errors.Is(err, graphpkg.ErrGraphClosed) {
			t.Fatalf("closed: %v", err)
		}
		if _, _, err := g.Stats().RelTypeTimeStats("R"); !errors.Is(err, graphpkg.ErrGraphClosed) {
			t.Fatalf("closed rel: %v", err)
		}
	})
}
