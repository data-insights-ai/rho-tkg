package core

import (
	"context"
	"fmt"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestReadBudget_DecodedBytes_TemporalPropertyLongHistory (READBUDGET, long
// histories): the node property Ctx doors with a time coordinate decode
// history versions to resolve each match at the coordinate. Under a budget of
// 1 decoded byte they stop at the first decode, whatever the number of matches
// and versions: ErrReadBudget "decoded bytes", no rows, and work bounded by
// one row's bytes, not by matches x versions.
func TestReadBudget_DecodedBytes_TemporalPropertyLongHistory(t *testing.T) {
	t.Parallel()
	const versions = 16
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			ctx := context.Background()
			if err := g.Index.CreateProperty("H", "k"); err != nil {
				t.Fatal(err)
			}
			groups := map[int][]types.NodeID{}
			for _, size := range []int{8, 32} {
				for i := range size {
					n, err := g.Nodes.Add(ctx, []string{"H"}, map[string]any{"k": int64(size), "pad": fmt.Sprintf("%0*d", 150, i)})
					if err != nil {
						t.Fatal(err)
					}
					for v := range versions {
						if _, err := g.Nodes.Update(ctx, n.ID(), map[string]any{"v": int64(v)}); err != nil {
							t.Fatal(err)
						}
					}
					groups[size] = append(groups[size], n.ID())
				}
			}
			pin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			shut()
			g, shut = be.open(t, dir)
			defer shut()

			for _, size := range []int{8, 32} {
				for _, o := range []struct {
					name string
					opts storepkg.QueryOpts
				}{
					{"TxPin", storepkg.QueryOpts{TxPin: pin}},
					{"TxAt", storepkg.QueryOpts{TxAt: pin}},
					{"ValidAt", storepkg.QueryOpts{ValidAt: pin, TxAt: pin}},
					{"interval", storepkg.QueryOpts{ValidStart: 1, ValidEnd: pin + 1, TxAt: pin}},
				} {
					rbdEvict(t, g, groups[size], nil)
					got0, err := g.Nodes.ByLabelAndPropertyCtx(ctx, "H", "k", int64(size), o.opts)
					got := got0
					if err != nil || len(got) != size {
						t.Fatalf("%d %s unbudgeted = %d rows, %v", size, o.name, len(got), err)
					}
					rbdEvict(t, g, groups[size], nil)
					bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{DecodedBytes: 1})
					got, err = g.Nodes.ByLabelAndPropertyCtx(bctx, "H", "k", int64(size), o.opts)
					w := meter.Work()
					if err == nil {
						t.Fatalf("%d matches x %d versions, %s: answered %d rows under a 1-byte budget, work %+v; want ErrReadBudget \"decoded bytes\"", size, versions, o.name, len(got), w)
					}
					be := rbdBudgetErr(t, err, "decoded bytes")
					if got != nil || be.Work.DecodedBytes > 4096 {
						t.Fatalf("%d %s: rows %d, work %+v; want none and one row's bytes", size, o.name, len(got), be.Work)
					}
					// Exact or refused: at its own count the read answers as
					// unbudgeted, one byte under it refuses with no rows.
					want := fmt.Sprint(nodeIDsOf(got0))
					rbdExact(t, fmt.Sprintf("%d %s", size, o.name), "ByLabelAndProperty", uint64(size)*150,
						func() { rbdEvict(t, g, groups[size], nil) },
						func(ctx context.Context) (bool, error) {
							r, err := g.Nodes.ByLabelAndPropertyCtx(ctx, "H", "k", int64(size), o.opts)
							return err == nil && fmt.Sprint(nodeIDsOf(r)) == want, err
						})
				}
			}
		})
	}
}
