package core

import (
	"context"
	"fmt"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestReadBudget_DecodedBytes_GetByIDs: GetByIDsCtx counts the stored bytes of
// the rows the store decodes to answer it. Cold, the count covers every row's
// wire bytes and is the same on every cold run; within a budget of exactly
// that count the door answers as without one; one byte under it returns
// ErrReadBudget "decoded bytes" and no rows, and nothing is decoded after the
// refusal. Warm, the entity cache serves the rows and the read counts 0. A
// single ID and a repeated ID read through the same count.
func TestReadBudget_DecodedBytes_GetByIDs(t *testing.T) {
	t.Parallel()
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			ctx := context.Background()
			const rows = 6
			var ids []types.NodeID
			for i := range rows {
				n, err := g.Nodes.Add(ctx, []string{"Ev"}, map[string]any{"pad": fmt.Sprintf("%0*d", 100+i, i)})
				if err != nil {
					t.Fatal(err)
				}
				ids = append(ids, n.ID())
			}
			shut()
			g, shut = be.open(t, dir)
			defer shut()

			for _, c := range []struct {
				name string
				ids  []types.NodeID
				want int // rows returned
			}{
				{"many", ids, rows},
				{"one", ids[:1], 1},
				{"repeated", []types.NodeID{ids[0], ids[1], ids[0]}, 3},
			} {
				min := uint64(c.want) * 100
				if c.name == "repeated" {
					min = 2 * 100 // the repeated row decodes once
				}
				rbdExact(t, "GetByIDs "+c.name, "GetByIDs", min,
					func() { rbdEvict(t, g, ids, nil) },
					func(ctx context.Context) (bool, error) {
						got, err := g.Nodes.GetByIDsCtx(ctx, c.ids)
						return len(got) == c.want, err
					})
			}
			// Warm: the entity cache serves the rows; nothing is decoded.
			bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{DecodedBytes: 1})
			if _, err := g.Nodes.GetByIDsCtx(context.Background(), ids); err != nil {
				t.Fatal(err)
			}
			got, err := g.Nodes.GetByIDsCtx(bctx, ids)
			if err != nil || len(got) != rows || meter.Work().DecodedBytes != 0 {
				t.Fatalf("warm GetByIDs = %d rows, %v, %+v; want the rows and 0 decoded bytes", len(got), err, meter.Work())
			}
		})
	}
}

// TestReadBudget_DecodedBytes_PresenceAndStampsDecodeNoRow: HasHistoryCtx and
// LatestStampsCtx read key presence and transaction stamps (RAM sidecars, and
// on a cache miss the temporal block of the current row), not a row: they count
// no decoded bytes, and a DecodedBytes limit never refuses them. Candidates
// still count.
func TestReadBudget_DecodedBytes_PresenceAndStampsDecodeNoRow(t *testing.T) {
	t.Parallel()
	for _, be := range rbdBackends() {
		t.Run(be.name, func(t *testing.T) {
			dir := t.TempDir()
			g, shut := be.open(t, dir)
			ctx := context.Background()
			n, err := g.Nodes.Add(ctx, []string{"Ev"}, map[string]any{"pad": fmt.Sprintf("%0*d", 500, 1)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Nodes.Update(ctx, n.ID(), map[string]any{"v": int64(1)}); err != nil {
				t.Fatal(err)
			}
			shut()
			g, shut = be.open(t, dir)
			defer shut()
			rbdEvict(t, g, []types.NodeID{n.ID()}, nil)
			bctx, meter := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{DecodedBytes: 1})
			if has, err := g.Nodes.HasHistoryCtx(bctx, n.ID()); err != nil || !has {
				t.Fatalf("HasHistoryCtx = %v, %v", has, err)
			}
			if _, _, _, err := g.Nodes.LatestStampsCtx(bctx, n.ID()); err != nil {
				t.Fatalf("LatestStampsCtx: %v", err)
			}
			if w := meter.Work(); w.DecodedBytes != 0 || w.Candidates != 2 {
				t.Fatalf("work %+v, want 2 candidates and 0 decoded bytes", w)
			}
		})
	}
}
