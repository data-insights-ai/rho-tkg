package core

import (
	"context"
	"fmt"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestReadBudget_DecodedBytes_LendByIDs: Nodes and Rels LendByIDs count the
// stored bytes of the rows the store decodes to lend them. Cold, the count
// covers every row's wire bytes and is the same on every cold run; within a
// budget of exactly that count the door lends as without one; one byte under
// it returns ErrReadBudget "decoded bytes" before fn is called, and nothing is
// decoded after the refusal.
func TestReadBudget_DecodedBytes_LendByIDs(t *testing.T) {
	t.Parallel()
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			ctx := context.Background()
			const rows = 6
			s, e := txbEndpoints(t, g)
			var nids []types.NodeID
			var rids []types.RelID
			for i := range rows {
				n, err := g.Nodes.Add(ctx, []string{"Ev"}, map[string]any{"pad": fmt.Sprintf("%0*d", 100+i, i)})
				if err != nil {
					t.Fatal(err)
				}
				nids = append(nids, n.ID())
				r, err := g.Rels.AddByID(ctx, "R", s, e, map[string]any{"pad": fmt.Sprintf("%0*d", 100+i, i)})
				if err != nil {
					t.Fatal(err)
				}
				rids = append(rids, r.ID())
			}
			shut()
			g, shut = be.open(t, dir)
			defer shut()

			for _, d := range []struct {
				name, store string
				lend        func(ctx context.Context, fn func()) error
			}{
				{"nodes", "LendNodesSorted", func(ctx context.Context, fn func()) error {
					return g.Nodes.LendByIDs(ctx, nids, func(int, *types.Node) bool { fn(); return true })
				}},
				{"rels", "LendRelationshipsSorted", func(ctx context.Context, fn func()) error {
					return g.Rels.LendByIDs(ctx, rids, func(int, *types.Relationship) bool { fn(); return true })
				}},
			} {
				lent := 0
				rbdExact(t, d.name, d.store, rows*100,
					func() { lent = 0; rbdEvict(t, g, nids, rids) },
					func(ctx context.Context) (bool, error) {
						err := d.lend(ctx, func() { lent++ })
						return lent == rows, err
					})
				if lent != 0 {
					t.Fatalf("%s: the refused lend called fn for %d rows", d.name, lent)
				}
			}
		})
	}
}

// TestReadBudget_DecodedBytes_LendByIDsWarm: tiered's lend fills the shard's
// entity cache, so a second lend of the same rows decodes nothing and counts 0
// (DecodedBytes counts decode work, not the size of the data read).
func TestReadBudget_DecodedBytes_LendByIDsWarm(t *testing.T) {
	t.Parallel()
	be := rbdBackends()[2]
	dir := t.TempDir()
	g, shut := be.open(t, dir)
	ctx := context.Background()
	var nids []types.NodeID
	for i := range 4 {
		n, err := g.Nodes.Add(ctx, []string{"Ev"}, map[string]any{"pad": fmt.Sprintf("%0*d", 100, i)})
		if err != nil {
			t.Fatal(err)
		}
		nids = append(nids, n.ID())
	}
	shut()
	g, shut = be.open(t, dir)
	defer shut()
	rbdEvict(t, g, nids, nil)
	lend := func() uint64 {
		bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
		if err := g.Nodes.LendByIDs(bctx, nids, func(int, *types.Node) bool { return true }); err != nil {
			t.Fatal(err)
		}
		return meter.Work().DecodedBytes
	}
	if cold, warm := lend(), lend(); cold < 4*100 || warm != 0 {
		t.Fatalf("cold lend counted %d bytes, warm %d; want a count, then 0", cold, warm)
	}
}

// TestReadBudget_DecodedBytes_MemoryDecodesNothing: the memory store keeps its
// rows decoded, so a metered door counts 0 decoded bytes there and a
// DecodedBytes limit never refuses (the count is decode work, which is none).
func TestReadBudget_DecodedBytes_MemoryDecodesNothing(t *testing.T) {
	t.Parallel()
	g := txbBackends()[0].open(t, false)
	ctx := context.Background()
	n, err := g.Nodes.Add(ctx, []string{"Ev"}, map[string]any{"pad": fmt.Sprintf("%0*d", 500, 1)})
	if err != nil {
		t.Fatal(err)
	}
	bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{DecodedBytes: 1})
	called := 0
	if err := g.Nodes.LendByIDs(bctx, []types.NodeID{n.ID()}, func(int, *types.Node) bool { called++; return true }); err != nil || called != 1 || meter.Work().DecodedBytes != 0 {
		t.Fatalf("memory lend = %d rows, %v, %+v; want the row and 0 decoded bytes", called, err, meter.Work())
	}
}
