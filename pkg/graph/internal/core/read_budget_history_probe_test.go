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

// TestReadBudget_HistoryExamined_LongHistory (READBUDGET step 2): the history
// versions a temporal property read looks at count as HistoryExamined before
// they are looked at. With 16 versions per match, a budget of 1 stops the read
// at its first version, whatever the number of matches (8, 32), on every
// backend (memory counts a chain before walking it); unlimited, the count
// grows with matches x versions and the answer is unchanged.
func TestReadBudget_HistoryExamined_LongHistory(t *testing.T) {
	t.Parallel()
	const versions = 16
	type open func(t *testing.T) (*Core, func())
	backends := []struct {
		name string
		open open
	}{{"memory", func(t *testing.T) (*Core, func()) { g := txbBackends()[0].open(t, false); return g, func() {} }}}
	for _, be := range rbdBackends() {
		backends = append(backends, struct {
			name string
			open open
		}{be.name, func(t *testing.T) (*Core, func()) { return be.open(t, t.TempDir()) }})
	}
	for _, be := range backends {
		t.Run(be.name, func(t *testing.T) {
			g, shut := be.open(t)
			defer shut()
			ctx := context.Background()
			if err := g.Index.CreateProperty("H", "k"); err != nil {
				t.Fatal(err)
			}
			var all []types.NodeID
			for _, size := range []int{8, 32} {
				for i := range size {
					n, err := g.Nodes.Add(ctx, []string{"H"}, map[string]any{"k": int64(size), "pad": fmt.Sprintf("%0*d", 50, i)})
					if err != nil {
						t.Fatal(err)
					}
					all = append(all, n.ID())
				}
			}
			// The pin sits in the middle of every node's history, so each
			// resolution walks the versions above it.
			var pin types.Instant
			for v := range versions {
				if v == versions/2 {
					p, err := g.Temporal.NowTx()
					if err != nil {
						t.Fatal(err)
					}
					pin = p
				}
				for _, id := range all {
					if _, err := g.Nodes.Update(ctx, id, map[string]any{"v": int64(v)}); err != nil {
						t.Fatal(err)
					}
				}
			}
			for _, size := range []int{8, 32} {
				for _, o := range []struct {
					name string
					opts storepkg.QueryOpts
				}{
					{"TxPin", storepkg.QueryOpts{TxPin: pin}},
					{"ValidAt", storepkg.QueryOpts{ValidAt: pin, TxAt: pin}},
				} {
					uctx, all := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
					want, err := g.Nodes.ByLabelAndPropertyCtx(uctx, "H", "k", int64(size), o.opts)
					if err != nil || len(want) != size || all.Work().HistoryExamined < uint64(size*versions/2) {
						t.Fatalf("%d %s unlimited = %d rows, %v, %+v; want %d rows and the versions above the pin counted", size, o.name, len(want), err, all.Work(), size)
					}
					bctx, one := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{HistoryExamined: 1})
					got, err := g.Nodes.ByLabelAndPropertyCtx(bctx, "H", "k", int64(size), o.opts)
					be := rbdBudgetErr(t, err, "history examined")
					// memory charges one chain (versions) at once; the disk
					// stores one version at a time.
					if got != nil || be.Work.HistoryExamined > versions+1 || one.Work() != be.Work {
						t.Fatalf("%d %s budget 1: rows %d, work %+v (unlimited %+v); want none, stopped at the first chain", size, o.name, len(got), be.Work, all.Work())
					}
					bctx, exact := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{HistoryExamined: all.Work().HistoryExamined})
					got, err = g.Nodes.ByLabelAndPropertyCtx(bctx, "H", "k", int64(size), o.opts)
					if err != nil || fmt.Sprint(nodeIDsOf(got)) != fmt.Sprint(nodeIDsOf(want)) || exact.Work().HistoryExamined != all.Work().HistoryExamined {
						t.Fatalf("%d %s at its own count: %d rows, %v, %+v", size, o.name, len(got), err, exact.Work())
					}
				}
			}
		})
	}
}
