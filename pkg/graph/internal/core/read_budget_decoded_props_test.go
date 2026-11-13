package core

import (
	"context"
	"fmt"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestReadBudget_DecodedBytes_NodePropertyDoors: the Ctx forms of
// ByLabelAndProperty, ByLabelAndPropertyValues and ByLabelAndProperties count
// the stored bytes of every row the store decodes to answer them (an index
// lookup's matches, a label scan's every row). Cold, the count covers the rows'
// wire bytes and is the same on every cold run; within a budget of exactly that
// count the door answers as without one; one byte under it returns
// ErrReadBudget "decoded bytes" and no rows, and nothing is decoded after the
// refusal.
func TestReadBudget_DecodedBytes_NodePropertyDoors(t *testing.T) {
	t.Parallel()
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			ctx := context.Background()
			if err := g.Index.CreateProperty("D", "k"); err != nil {
				t.Fatal(err)
			}
			const nodes = 20
			var ids []types.NodeID
			for i := range nodes {
				n, err := g.Nodes.Add(ctx, []string{"D"}, map[string]any{
					"k": int64(i % 5), "u": int64(i), "v": int64(i % 3), "pad": fmt.Sprintf("%0*d", 100, i),
				})
				if err != nil {
					t.Fatal(err)
				}
				ids = append(ids, n.ID())
			}
			shut()
			g, shut = be.open(t, dir)
			defer shut()

			for i, d := range []struct {
				name string
				call func(ctx context.Context) (int, error)
				want int
			}{
				{"ByLabelAndProperty", func(c context.Context) (int, error) { // indexed: 4 matches
					r, err := g.Nodes.ByLabelAndPropertyCtx(c, "D", "k", int64(1), storepkg.QueryOpts{})
					return len(r), err
				}, 4},
				{"ByLabelAndPropertyValues", func(c context.Context) (int, error) { // indexed: 8 matches
					r, err := g.Nodes.ByLabelAndPropertyValuesCtx(c, "D", "k", []any{int64(1), int64(2)}, storepkg.QueryOpts{})
					return len(r), err
				}, 8},
				{"ByLabelAndProperty", func(c context.Context) (int, error) { // no index on u: the label scan reads every row
					r, err := g.Nodes.ByLabelAndPropertyCtx(c, "D", "u", int64(7), storepkg.QueryOpts{})
					return len(r), err
				}, 1},
				{"ByLabelAndProperties", func(c context.Context) (int, error) { // no composite index: the label scan
					r, err := g.Nodes.ByLabelAndPropertiesCtx(c, "D", map[string]any{"u": int64(7), "v": int64(1)}, storepkg.QueryOpts{})
					return len(r), err
				}, 1},
			} {
				rbdExact(t, fmt.Sprintf("door %d %s", i, d.name), d.name, uint64(d.want)*100,
					func() { rbdEvict(t, g, ids, nil) },
					func(ctx context.Context) (bool, error) {
						n, err := d.call(ctx)
						return n == d.want, err
					})
			}
		})
	}
}

// TestReadBudget_DecodedBytes_NodePropertyNoByteLimit: a budget without a
// DecodedBytes limit never refuses on bytes, however large the rows.
func TestReadBudget_DecodedBytes_NodePropertyNoByteLimit(t *testing.T) {
	t.Parallel()
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			ctx := context.Background()
			if err := g.Index.CreateProperty("D", "k"); err != nil {
				t.Fatal(err)
			}
			for i := range 6 {
				if _, err := g.Nodes.Add(ctx, []string{"D"}, map[string]any{"k": int64(i % 2), "pad": fmt.Sprintf("%0*d", 3000, i)}); err != nil {
					t.Fatal(err)
				}
			}
			shut()
			g, shut = be.open(t, dir)
			defer shut()
			bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: 3})
			got, err := g.Nodes.ByLabelAndPropertyCtx(bctx, "D", "k", int64(1), storepkg.QueryOpts{})
			if err != nil || len(got) != 3 || meter.Work().Candidates != 3 || meter.Work().DecodedBytes < 3*3000 {
				t.Fatalf("budget without a byte limit = %d, %v, %+v", len(got), err, meter.Work())
			}
		})
	}
}
