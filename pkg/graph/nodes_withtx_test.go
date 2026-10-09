package graph_test

import (
	"context"
	"errors"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The public caller-instant node doors (Nodes().DeleteWithTx,
// Nodes().UpdateWithTx) through the pkg/graph facade on every backend.
//
// Catches: a facade that forwards to the plain Delete/Update (t equal to
// TxFrom would then succeed, and the cascaded relationship would carry the
// clock), a zero instant read as "use the clock", and a refusal that still
// writes. Counterpart: TxFrom+1 is stamped exactly on the node and, for the
// delete, on the cascaded relationship.
func TestNodesWithTx_FacadeRefusesAndStampsT(t *testing.T) {
	t.Parallel()
	backends := allStoreBackendsWith(func(c *graphpkg.Config) { c.AllowTxBackfill = true })
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			base := types.Instant(time.Now().Add(-2 * time.Hour).UnixMilli())
			for _, door := range []struct {
				name string
				run  func(id types.NodeID, at types.Instant) error
			}{
				{"DeleteWithTx", func(id types.NodeID, at types.Instant) error { return g.Nodes().DeleteWithTx(ctx, id, at) }},
				{"UpdateWithTx", func(id types.NodeID, at types.Instant) error {
					_, err := g.Nodes().UpdateWithTx(ctx, id, map[string]any{"w": int64(2)}, at)
					return err
				}},
			} {
				n, err := g.Nodes().AddWithTx(ctx, []string{"Ref"}, map[string]any{"tkg_valid_from": base - 1000, "w": int64(1)}, base)
				if err != nil {
					t.Fatalf("AddWithTx: %v", err)
				}
				ev, err := g.Nodes().AddWithTx(ctx, []string{"Ev"}, map[string]any{"tkg_valid_from": base - 1000}, base)
				if err != nil {
					t.Fatalf("AddWithTx: %v", err)
				}
				r, err := g.Rels().AddWithTx(ctx, "LINK", n, ev, map[string]any{"tkg_valid_from": base - 1000}, base)
				if err != nil {
					t.Fatalf("Rels.AddWithTx: %v", err)
				}
				for _, tc := range []struct {
					at    types.Instant
					order bool
				}{{0, false}, {base, true}, {base - 1, true}} {
					err := door.run(n.ID(), tc.at)
					if !errors.Is(err, graphpkg.ErrInvalidTxFrom) || errors.Is(err, graphpkg.ErrTxOrder) != tc.order {
						t.Fatalf("%s(t=%d) err = %v; want ErrInvalidTxFrom, ErrTxOrder=%v", door.name, tc.at, err, tc.order)
					}
					cur, err := g.Nodes().Get(ctx, n.ID())
					if err != nil || cur.Version() != n.Version() || cur.Temporal().TxFrom != base {
						t.Fatalf("%s(t=%d) refused but the node changed: %v, %v", door.name, tc.at, cur, err)
					}
					if _, err := g.Rels().Get(ctx, r.ID()); err != nil {
						t.Fatalf("%s(t=%d) refused but the cascaded rel is gone: %v", door.name, tc.at, err)
					}
				}
				if err := door.run(n.ID(), base+1); err != nil {
					t.Fatalf("%s(TxFrom+1): %v", door.name, err)
				}
				hist, err := g.Nodes().History(n.ID())
				if err != nil || len(hist) != 1 || hist[0].Temporal().TxTo != base+1 {
					t.Fatalf("%s(TxFrom+1): history %v, %v; want one row with TxTo = %d", door.name, hist, err, base+1)
				}
				if door.name == "DeleteWithTx" {
					rh, err := g.Rels().History(r.ID())
					if err != nil || len(rh) != 1 || rh[0].Temporal().TxTo != base+1 {
						t.Fatalf("cascaded rel history %v, %v; want one tombstone with TxTo = %d", rh, err, base+1)
					}
				}
			}
		})
	}
}
