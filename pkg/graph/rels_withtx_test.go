package graph_test

import (
	"context"
	"errors"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The public caller-instant relationship doors (Rels().DeleteWithTx,
// Rels().UpdateWithTx) through the pkg/graph facade on every backend.
//
// Catches: ErrTxOrder not reachable from pkg/graph or not matching
// ErrInvalidTxFrom (a consumer mapping ErrInvalidTxFrom to a validation error
// would then miss it), a facade that forwards to the plain Delete/Update (a t
// equal to TxFrom would then succeed), a zero instant read as "use the clock",
// and a refusal that still writes. Counterpart: TxFrom+1 is stamped exactly.
func TestRelsWithTx_FacadeRefusesAndStampsT(t *testing.T) {
	t.Parallel()
	if !errors.Is(graphpkg.ErrTxOrder, graphpkg.ErrInvalidTxFrom) {
		t.Fatal("graph.ErrTxOrder must wrap graph.ErrInvalidTxFrom")
	}
	backends := allStoreBackendsWith(func(c *graphpkg.Config) { c.AllowTxBackfill = true })
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			start, err := g.Nodes().Add(ctx, []string{"Ref"}, nil)
			if err != nil {
				t.Fatalf("Add: %v", err)
			}
			end, err := g.Nodes().Add(ctx, []string{"Ev"}, nil)
			if err != nil {
				t.Fatalf("Add: %v", err)
			}
			base := types.Instant(time.Now().Add(-2 * time.Hour).UnixMilli())
			for _, door := range []struct {
				name string
				run  func(id types.RelID, at types.Instant) error
			}{
				{"DeleteWithTx", func(id types.RelID, at types.Instant) error { return g.Rels().DeleteWithTx(ctx, id, at) }},
				{"UpdateWithTx", func(id types.RelID, at types.Instant) error {
					_, err := g.Rels().UpdateWithTx(ctx, id, map[string]any{"w": int64(2)}, at)
					return err
				}},
			} {
				r, err := g.Rels().AddWithTx(ctx, "LINK", start, end, map[string]any{"tkg_valid_from": base - 1000, "w": int64(1)}, base)
				if err != nil {
					t.Fatalf("AddWithTx: %v", err)
				}
				for _, tc := range []struct {
					at    types.Instant
					order bool
				}{{0, false}, {base, true}, {base - 1, true}} {
					err := door.run(r.ID(), tc.at)
					if !errors.Is(err, graphpkg.ErrInvalidTxFrom) || errors.Is(err, graphpkg.ErrTxOrder) != tc.order {
						t.Fatalf("%s(t=%d) err = %v; want ErrInvalidTxFrom, ErrTxOrder=%v", door.name, tc.at, err, tc.order)
					}
					cur, err := g.Rels().Get(ctx, r.ID())
					if err != nil || cur.Version() != r.Version() || cur.Temporal().TxFrom != base {
						t.Fatalf("%s(t=%d) refused but the row changed: %v, %v", door.name, tc.at, cur, err)
					}
				}
				if err := door.run(r.ID(), base+1); err != nil {
					t.Fatalf("%s(TxFrom+1): %v", door.name, err)
				}
				hist, err := g.Rels().History(r.ID())
				if err != nil || len(hist) != 1 || hist[0].Temporal().TxTo != base+1 {
					t.Fatalf("%s(TxFrom+1): history %v, %v; want one row with TxTo = %d", door.name, hist, err, base+1)
				}
			}
		})
	}
}
