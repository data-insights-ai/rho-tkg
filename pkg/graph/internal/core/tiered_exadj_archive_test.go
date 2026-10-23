package core

import (
	"context"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// F2 (v4.51.2 review): a relationship of an archived node, deleted while the
// node is archived, leaves its ex-adjacency on the archive; a door pinned
// before the delete must still reach it from the archived node, in both
// directions (tiered reads the archive's ex-adjacency, tieredstore_exadj.go).
func TestTieredArchivedNodeRelDeletePinnedDoor(t *testing.T) {
	t.Parallel()
	g, ts := newTestTieredGraph(t)
	ctx := context.Background()
	x, err := g.Nodes.Add(ctx, []string{"Case"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	y, err := g.Nodes.Add(ctx, []string{"Case"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := ts.ArchiveNode(x.ID()); err != nil {
		t.Fatal(err)
	}
	in, err := g.Rels.Add(ctx, "R", y, x, nil) // in-leg on the archive
	if err != nil {
		t.Fatal(err)
	}
	out, err := g.Rels.Add(ctx, "R", x, y, nil) // entity and out-leg on the archive
	if err != nil {
		t.Fatal(err)
	}
	before, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range []types.RelID{in.ID(), out.ID()} {
		if err := g.Rels.Delete(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	after, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		incoming bool
		rel      types.RelID
	}{{true, in.ID()}, {false, out.ID()}} {
		if got := adjRelsAt(t, g, x.ID(), "R", c.incoming, storepkg.QueryOpts{TxPin: before}); len(got) != 1 || !hasKey(got, c.rel) {
			t.Errorf("incoming=%v pinned before the delete: %v, want exactly %d", c.incoming, got, c.rel)
		}
		if got := adjRelsAt(t, g, x.ID(), "R", c.incoming, storepkg.QueryOpts{TxPin: after}); len(got) != 0 {
			t.Errorf("incoming=%v pinned after the delete: %v, want none", c.incoming, got)
		}
	}
}

func hasKey(m map[types.RelID]any, k types.RelID) bool { _, ok := m[k]; return ok }
